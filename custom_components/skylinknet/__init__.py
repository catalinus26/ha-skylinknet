"""SkylinkNet integration."""

from __future__ import annotations

import logging

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import (
    ConfigEntryAuthFailed,
    ConfigEntryNotReady,
    HomeAssistantError,
)
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import SkylinkNetApi, SkylinkNetAuthError
from .const import (
    CONF_CONFIG_ENTRY_ID,
    CONF_IGNORE_FUTURE_EVENTS,
    CONF_SKYLINKNET_DEVICE_ID,
    DOMAIN,
    SERVICE_ALLOW_DEVICE,
    SERVICE_FORGET_DEVICE,
)
from .coordinator import SkylinkNetCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [
    "binary_sensor",
    "alarm_control_panel",
    "sensor",
]


# ============================================================
# SERVICES
# ============================================================

FORGET_DEVICE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_SKYLINKNET_DEVICE_ID): cv.string,
        vol.Optional(CONF_CONFIG_ENTRY_ID): cv.string,
        vol.Optional(
            CONF_IGNORE_FUTURE_EVENTS,
            default=True,
        ): cv.boolean,
    }
)

ALLOW_DEVICE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_SKYLINKNET_DEVICE_ID): cv.string,
        vol.Optional(CONF_CONFIG_ENTRY_ID): cv.string,
    }
)


# ============================================================
# SETUP
# ============================================================

async def async_setup(
    hass: HomeAssistant,
    config: dict,
) -> bool:
    """Set up SkylinkNet."""

    hass.data.setdefault(DOMAIN, {})

    _async_register_services(hass)

    return True


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> bool:
    """Set up SkylinkNet from a config entry."""

    _async_register_services(hass)

    session = async_get_clientsession(hass)

    api = SkylinkNetApi(
        session,
        entry.data["email"],
        entry.data["password"],
    )

    api.hub_id = entry.data["hub_id"]
    api.hub_key = entry.data["hub_key"]

    # =========================================================
    # INITIAL API DATA
    # =========================================================

    try:
        login = await api.login()

        _LOGGER.debug(
            "SkylinkNet login OK: user_id=%s",
            login.get("data", {}).get("user_id"),
        )

        status = await api.get_status()

        _LOGGER.debug(
            "SkylinkNet initial status: %s",
            status,
        )

        devices = await api.get_devices()

        _LOGGER.debug(
            "SkylinkNet devices: %s",
            devices,
        )

        read = await api.read_devices()

        _LOGGER.debug(
            "SkylinkNet initial read: %s",
            read,
        )

    except SkylinkNetAuthError as err:
        _LOGGER.warning(
            "SkylinkNet authentication failed, "
            "reauthentication required"
        )

        raise ConfigEntryAuthFailed(
            f"Authentication failed: {err}"
        ) from err

    except Exception as err:
        _LOGGER.exception(
            "SkylinkNet API initialization failed"
        )

        raise ConfigEntryNotReady(
            f"Unable to connect to SkylinkNet: {err}"
        ) from err

    # =========================================================
    # COORDINATOR
    # =========================================================

    coordinator = SkylinkNetCoordinator(
        hass,
        api,
        entry.entry_id,
    )

    # =========================================================
    # PERSISTED DATA
    # =========================================================

    await coordinator.async_load_persisted()

    # =========================================================
    # DEVICE CONFIGURATION
    # =========================================================

    persisted_changed = False

    for device in devices.get("data", []):
        if not isinstance(device, dict):
            continue

        dev_id = device.get("dev_id")

        if not dev_id:
            continue

        if dev_id == coordinator.ALARM_DEVICE_ID:
            continue

        if dev_id in coordinator.ignored_device_ids:
            continue

        coordinator.devices[dev_id] = dict(device)

        if dev_id not in coordinator.known_device_ids:
            coordinator.known_device_ids.add(dev_id)
            persisted_changed = True

    # =========================================================
    # INITIAL STATES
    #
    # Important:
    # F0000000 is the virtual SkylinkNet alarm device.
    # It must NOT become a normal binary sensor.
    #
    # Its status IS used by the coordinator to restore the
    # alarm_control_panel state after Home Assistant restart.
    # =========================================================

    for item in read.get("data", []):
        if not isinstance(item, dict):
            continue

        dev_id = item.get("dev_id")

        if not dev_id:
            continue

        if dev_id == coordinator.ALARM_DEVICE_ID:
            continue

        if dev_id in coordinator.ignored_device_ids:
            continue

        coordinator.states[dev_id] = dict(item)

        if dev_id not in coordinator.known_device_ids:
            coordinator.known_device_ids.add(dev_id)
            persisted_changed = True

    # =========================================================
    # INITIAL ALARM STATE
    #
    # IMPORTANT:
    # get_hub_status does NOT contain the alarm state.
    #
    # read() contains:
    #
    #   F0000000 status=4 -> disarmed
    #   F0000000 status=3 -> armed away
    #
    # Therefore the alarm state must be restored from "read".
    # =========================================================

    coordinator.update_alarm_state_from_read(read)

    if persisted_changed:
        await coordinator.async_save_persisted()

    # =========================================================
    # STORE
    # =========================================================

    hass.data.setdefault(DOMAIN, {})

    hass.data[DOMAIN][entry.entry_id] = {
        "api": api,
        "coordinator": coordinator,
    }

    # =========================================================
    # REGISTER HUB DEVICE
    #
    # Physical SkylinkNet devices use the hub as via_device.
    # The hub must exist in the device registry before the
    # platform entities are created.
    # =========================================================

    device_registry = dr.async_get(hass)

    device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={
            (
                DOMAIN,
                str(api.hub_id),
            )
        },
        name=f"SkylinkNet Hub {api.hub_id}",
        manufacturer="SkylinkNet",
    )

    # =========================================================
    # CREATE ENTITIES
    # =========================================================

    await hass.config_entries.async_forward_entry_setups(
        entry,
        PLATFORMS,
    )

    # =========================================================
    # START WEBSOCKET
    # =========================================================

    await coordinator.start()

    async def _async_stop_coordinator(event) -> None:
        """Stop coordinator when Home Assistant shuts down."""

        await coordinator.stop()

    entry.async_on_unload(
        hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP,
            _async_stop_coordinator,
        )
    )

    # =========================================================
    # RELOAD ON OPTIONS CHANGE
    # =========================================================

    entry.async_on_unload(
        entry.add_update_listener(
            _async_update_listener
        )
    )

    _LOGGER.info(
        "SkylinkNet initialized - WebSocket monitoring started"
    )

    return True


# ============================================================
# UNLOAD
# ============================================================

async def async_unload_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> bool:
    """Unload SkylinkNet."""

    data = hass.data.get(DOMAIN, {}).get(entry.entry_id)

    if data:
        coordinator = data.get("coordinator")

        if coordinator:
            await coordinator.stop()

    unload_ok = await hass.config_entries.async_unload_platforms(
        entry,
        PLATFORMS,
    )

    if unload_ok:
        hass.data.get(DOMAIN, {}).pop(
            entry.entry_id,
            None,
        )

    return unload_ok


async def _async_update_listener(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> None:
    """Reload entry after options change."""

    await hass.config_entries.async_reload(
        entry.entry_id
    )


# ============================================================
# REMOVE DEVICE
# ============================================================

async def async_remove_config_entry_device(
    hass: HomeAssistant,
    entry: ConfigEntry,
    device_entry: dr.DeviceEntry,
) -> bool:
    """Allow users to remove a stale SkylinkNet device from UI."""

    data = hass.data.get(DOMAIN, {}).get(entry.entry_id)

    if not data:
        return False

    coordinator: SkylinkNetCoordinator = data["coordinator"]

    dev_id = None

    for identifier in device_entry.identifiers:
        if len(identifier) == 2 and identifier[0] == DOMAIN:
            candidate = identifier[1]

            # Exclude the hub itself.
            if candidate != str(coordinator.api.hub_id):
                dev_id = candidate

            break

    if dev_id is None:
        return False

    await coordinator.async_forget_device(
        dev_id,
        ignore_future=True,
    )

    return True


# ============================================================
# SERVICE HANDLERS
# ============================================================

def _async_register_services(
    hass: HomeAssistant,
) -> None:
    """Register SkylinkNet management services."""

    if not hass.services.has_service(
        DOMAIN,
        SERVICE_FORGET_DEVICE,
    ):
        hass.services.async_register(
            DOMAIN,
            SERVICE_FORGET_DEVICE,
            _async_make_forget_device_service(hass),
            schema=FORGET_DEVICE_SCHEMA,
        )

    if not hass.services.has_service(
        DOMAIN,
        SERVICE_ALLOW_DEVICE,
    ):
        hass.services.async_register(
            DOMAIN,
            SERVICE_ALLOW_DEVICE,
            _async_make_allow_device_service(hass),
            schema=ALLOW_DEVICE_SCHEMA,
        )


def _async_make_forget_device_service(
    hass: HomeAssistant,
):
    """Build forget_device service handler."""

    async def _async_forget_device_service(
        call: ServiceCall,
    ) -> None:
        """Forget a SkylinkNet device."""

        dev_id: str = call.data[
            CONF_SKYLINKNET_DEVICE_ID
        ]

        config_entry_id: str | None = call.data.get(
            CONF_CONFIG_ENTRY_ID
        )

        ignore_future: bool = call.data[
            CONF_IGNORE_FUTURE_EVENTS
        ]

        matched = False

        for coordinator in _matching_coordinators(
            hass,
            config_entry_id,
        ):
            if await coordinator.async_forget_device(
                dev_id,
                ignore_future,
            ):
                matched = True

        if not matched:
            raise HomeAssistantError(
                f"SkylinkNet device {dev_id} was not found"
            )

    return _async_forget_device_service


def _async_make_allow_device_service(
    hass: HomeAssistant,
):
    """Build allow_device service handler."""

    async def _async_allow_device_service(
        call: ServiceCall,
    ) -> None:
        """Allow a previously forgotten SkylinkNet device."""

        dev_id: str = call.data[
            CONF_SKYLINKNET_DEVICE_ID
        ]

        config_entry_id: str | None = call.data.get(
            CONF_CONFIG_ENTRY_ID
        )

        matched = False

        for coordinator in _matching_coordinators(
            hass,
            config_entry_id,
        ):
            if await coordinator.async_allow_device(
                dev_id
            ):
                matched = True

        if not matched:
            raise HomeAssistantError(
                f"SkylinkNet device {dev_id} was not ignored"
            )

    return _async_allow_device_service


def _matching_coordinators(
    hass: HomeAssistant,
    config_entry_id: str | None,
) -> list[SkylinkNetCoordinator]:
    """Return coordinators matching optional config entry ID."""

    all_data: dict = hass.data.get(
        DOMAIN,
        {},
    )

    if config_entry_id is None:
        return [
            data["coordinator"]
            for data in all_data.values()
        ]

    if config_entry_id not in all_data:
        raise HomeAssistantError(
            f"SkylinkNet config entry "
            f"{config_entry_id} was not found"
        )

    return [
        all_data[config_entry_id]["coordinator"]
    ]