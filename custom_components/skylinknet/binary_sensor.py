"""Binary sensors for SkylinkNet."""

from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import (
    HomeAssistant,
    callback,
)
from homeassistant.helpers.entity_platform import (
    AddEntitiesCallback,
)

from .const import (
    ATTR_DEVICE_ID,
    CONF_DEFAULT_DEVICE_CLASS,
    DEFAULT_DEVICE_CLASS,
    DEV_TYPE_DOOR,
    DEV_TYPE_MOTION,
    DEV_TYPE_WINDOW,
    DEVICE_CLASS_NONE,
    DOMAIN,
)
from .coordinator import SkylinkNetCoordinator


# ============================================================
# DEVICE TYPE -> HOME ASSISTANT DEVICE CLASS
# ============================================================

_DEV_TYPE_CLASS_MAP = {
    DEV_TYPE_DOOR: BinarySensorDeviceClass.DOOR,
    DEV_TYPE_WINDOW: BinarySensorDeviceClass.WINDOW,
    DEV_TYPE_MOTION: BinarySensorDeviceClass.MOTION,
}


# ============================================================
# SETUP
# ============================================================

async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up SkylinkNet binary sensors."""

    data = hass.data[
        DOMAIN
    ][entry.entry_id]

    coordinator: SkylinkNetCoordinator = (
        data["coordinator"]
    )

    known_entities: set[str] = set()

    def _device_class_for(
        dev_id: str,
    ) -> BinarySensorDeviceClass | None:
        """Return best device class for a device."""

        device = (
            coordinator.get_device(dev_id)
            or {}
        )

        try:
            dev_type = int(
                device.get(
                    "dev_type",
                    -1,
                )
            )
        except (
            TypeError,
            ValueError,
        ):
            dev_type = -1

        if dev_type in _DEV_TYPE_CLASS_MAP:
            return _DEV_TYPE_CLASS_MAP[
                dev_type
            ]

        default_class = entry.options.get(
            CONF_DEFAULT_DEVICE_CLASS,
            DEFAULT_DEVICE_CLASS,
        )

        if default_class == DEVICE_CLASS_NONE:
            return None

        try:
            return BinarySensorDeviceClass(
                default_class
            )
        except ValueError:
            return None

    @callback
    def _async_add_device(
        dev_id: str,
    ) -> None:
        """Create binary sensor if not already created."""

        # Never create an entity for the virtual alarm device.
        if (
            dev_id
            == coordinator.ALARM_DEVICE_ID
        ):
            return

        # Never create an entity for ignored devices.
        if (
            dev_id
            in coordinator.ignored_device_ids
        ):
            return

        if dev_id in known_entities:
            return

        known_entities.add(
            dev_id
        )

        async_add_entities(
            [
                SkylinkNetBinarySensor(
                    coordinator,
                    dev_id,
                    _device_class_for(
                        dev_id
                    ),
                )
            ]
        )

    # ============================================================
    # KNOWN DEVICES
    # ============================================================

    for dev_id in list(
        coordinator.devices.keys()
    ) + list(
        coordinator.known_device_ids
    ):

        _async_add_device(
            dev_id
        )

    # ============================================================
    # DYNAMIC DISCOVERY
    # ============================================================

    coordinator.add_new_device_listener(
        _async_add_device
    )

    entry.async_on_unload(
        lambda: coordinator.remove_new_device_listener(
            _async_add_device
        )
    )

    # ============================================================
    # HUB ALARM / WEBSOCKET ENTITIES
    # ============================================================

    async_add_entities(
        [
            SkylinkNetWebSocketSensor(
                coordinator
            ),
            SkylinkNetAlarmArmedSensor(
                coordinator
            ),
            SkylinkNetAlarmTriggeredSensor(
                coordinator
            ),
        ]
    )


# ============================================================
# DEVICE BINARY SENSOR
# ============================================================

class SkylinkNetBinarySensor(
    BinarySensorEntity
):
    """SkylinkNet binary sensor."""

    _attr_has_entity_name = True
    _attr_name = None
    _attr_should_poll = False

    def __init__(
        self,
        coordinator: SkylinkNetCoordinator,
        dev_id: str,
        device_class: BinarySensorDeviceClass | None,
    ) -> None:
        """Initialize sensor."""

        self.coordinator = coordinator

        self._dev_id = dev_id

        self._attr_device_class = (
            device_class
        )

        self._attr_unique_id = (
            f"skylinknet_{self._dev_id}"
        )

        self._attr_device_info = (
            coordinator.device_info_for(
                self._dev_id
            )
        )

        coordinator.add_listener(
            self._state_changed
        )

        coordinator.add_monitor_listener(
            self._connection_changed
        )

    @property
    def is_on(
        self,
    ) -> bool:
        """Return current sensor state."""

        state = self.coordinator.get_state(
            self._dev_id
        )

        try:
            status = int(
                state.get(
                    "status",
                    0,
                )
            )
        except (
            TypeError,
            ValueError,
        ):
            status = 0

        return status == 1

    @property
    def available(
        self,
    ) -> bool:
        """Return false when WebSocket is disconnected."""

        return (
            self.coordinator.websocket_connected
        )

    @property
    def extra_state_attributes(
        self,
    ) -> dict[str, object]:
        """Return diagnostic attributes."""

        return {
            ATTR_DEVICE_ID: self._dev_id
        }

    @callback
    def _state_changed(
        self,
        dev_id: str,
    ) -> None:
        """Handle WebSocket state update."""

        if dev_id != self._dev_id:
            return

        self.async_write_ha_state()

    @callback
    def _connection_changed(
        self,
    ) -> None:
        """Handle WebSocket connection change."""

        self.async_write_ha_state()

    async def async_will_remove_from_hass(
        self,
    ) -> None:
        """Remove listeners."""

        self.coordinator.remove_listener(
            self._state_changed
        )

        self.coordinator.remove_monitor_listener(
            self._connection_changed
        )

        await super().async_will_remove_from_hass()


# ============================================================
# WEBSOCKET CONNECTION SENSOR
# ============================================================

class SkylinkNetWebSocketSensor(
    BinarySensorEntity
):
    """SkylinkNet WebSocket connection status."""

    _attr_device_class = (
        BinarySensorDeviceClass.CONNECTIVITY
    )

    _attr_has_entity_name = True
    _attr_name = "WebSocket"
    _attr_should_poll = False

    def __init__(
        self,
        coordinator: SkylinkNetCoordinator,
    ) -> None:
        """Initialize."""

        self.coordinator = coordinator

        self._attr_unique_id = (
            f"skylinknet_"
            f"{coordinator.api.hub_id}"
            "_websocket"
        )

        self._attr_device_info = (
            coordinator.hub_device_info
        )

        coordinator.add_monitor_listener(
            self._state_changed
        )

    @property
    def is_on(
        self,
    ) -> bool:
        """Return true when WebSocket is connected."""

        return (
            self.coordinator.websocket_connected
        )

    @callback
    def _state_changed(
        self,
    ) -> None:
        """Handle WebSocket state change."""

        self.async_write_ha_state()

    async def async_will_remove_from_hass(
        self,
    ) -> None:
        """Remove listener."""

        self.coordinator.remove_monitor_listener(
            self._state_changed
        )

        await super().async_will_remove_from_hass()


# ============================================================
# ALARM ARMED
# ============================================================

class SkylinkNetAlarmArmedSensor(
    BinarySensorEntity
):
    """SkylinkNet alarm armed status."""

    _attr_has_entity_name = True
    _attr_name = "Alarm Armed"
    _attr_should_poll = False
    _attr_icon = "mdi:shield-lock"

    def __init__(
        self,
        coordinator: SkylinkNetCoordinator,
    ) -> None:
        """Initialize."""

        self.coordinator = coordinator

        self._attr_unique_id = (
            f"skylinknet_"
            f"{coordinator.api.hub_id}"
            "_alarm_armed"
        )

        self._attr_device_info = (
            coordinator.hub_device_info
        )

        coordinator.add_monitor_listener(
            self._state_changed
        )

    @property
    def is_on(
        self,
    ) -> bool:
        """Return true when alarm is armed."""

        return self.coordinator._arming_mode in (
            "armed_home",
            "armed_away",
        )

    @callback
    def _state_changed(
        self,
    ) -> None:
        """Handle alarm state change."""

        self.async_write_ha_state()

    async def async_will_remove_from_hass(
        self,
    ) -> None:
        """Remove listener."""

        self.coordinator.remove_monitor_listener(
            self._state_changed
        )

        await super().async_will_remove_from_hass()


# ============================================================
# ALARM TRIGGERED
# ============================================================

class SkylinkNetAlarmTriggeredSensor(
    BinarySensorEntity
):
    """SkylinkNet alarm triggered status."""

    _attr_has_entity_name = True
    _attr_name = "Alarm Triggered"
    _attr_should_poll = False
    _attr_device_class = (
        BinarySensorDeviceClass.PROBLEM
    )

    def __init__(
        self,
        coordinator: SkylinkNetCoordinator,
    ) -> None:
        """Initialize."""

        self.coordinator = coordinator

        self._attr_unique_id = (
            f"skylinknet_"
            f"{coordinator.api.hub_id}"
            "_alarm_triggered"
        )

        self._attr_device_info = (
            coordinator.hub_device_info
        )

        coordinator.add_monitor_listener(
            self._state_changed
        )

    @property
    def is_on(
        self,
    ) -> bool:
        """Return true when alarm is triggered."""

        return (
            self.coordinator.alarm_state
            == "triggered"
        )

    @callback
    def _state_changed(
        self,
    ) -> None:
        """Handle alarm state change."""

        self.async_write_ha_state()

    async def async_will_remove_from_hass(
        self,
    ) -> None:
        """Remove listener."""

        self.coordinator.remove_monitor_listener(
            self._state_changed
        )

        await super().async_will_remove_from_hass()
