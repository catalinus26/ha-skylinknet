"""Diagnostics for SkylinkNet."""

from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.redact import async_redact_data

from .const import DOMAIN
from .sanitize import DIAGNOSTICS_TO_REDACT, scrub_deep

# NOTE: "hub_id" is intentionally NOT in DIAGNOSTICS_TO_REDACT — it is
# an identifier, not a credential (see sanitize.py).
TO_REDACT = DIAGNOSTICS_TO_REDACT


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    """Return diagnostics for a SkylinkNet config entry."""

    data = hass.data.get(DOMAIN, {}).get(entry.entry_id)

    coordinator = data.get("coordinator") if data else None
    api = data.get("api") if data else None

    secrets: tuple[str, ...] = api.secrets if api else ()

    result: dict[str, Any] = {
        "entry": {
            "data": async_redact_data(entry.data, TO_REDACT),
            "options": dict(entry.options),
        },
        "runtime": {
            "api": (
                api.get_diagnostics_info()
                if api
                else None
            ),
            "websocket": (
                coordinator.get_websocket_info()
                if coordinator
                else None
            ),
            "alarm": {
                "state": (
                    coordinator.alarm_state if coordinator else None
                ),
                "last_arm": (
                    coordinator.get_last_arm_info()
                    if coordinator
                    else None
                ),
            },
            "devices": {
                "known_device_count": (
                    len(coordinator.known_device_ids)
                    if coordinator
                    else 0
                ),
                "ignored_device_count": (
                    len(coordinator.ignored_device_ids)
                    if coordinator
                    else 0
                ),
                "device_count": (
                    len(coordinator.devices) if coordinator else 0
                ),
            },
            # Kept for anything already reading the old flat keys.
            "websocket_connected": (
                coordinator.websocket_connected if coordinator else None
            ),
            "websocket_connect_count": (
                coordinator.websocket_connect_count if coordinator else None
            ),
            "websocket_disconnect_count": (
                coordinator.websocket_disconnect_count
                if coordinator
                else None
            ),
            "websocket_reconnect_count": (
                coordinator.websocket_reconnect_count
                if coordinator
                else None
            ),
            "websocket_message_count": (
                coordinator.websocket_message_count if coordinator else None
            ),
            "websocket_error_count": (
                coordinator.websocket_error_count if coordinator else None
            ),
            "websocket_last_error": (
                coordinator.websocket_last_error if coordinator else None
            ),
            "alarm_state": coordinator.alarm_state if coordinator else None,
            "known_device_count": (
                len(coordinator.known_device_ids) if coordinator else 0
            ),
            "ignored_device_count": (
                len(coordinator.ignored_device_ids) if coordinator else 0
            ),
            "device_count": len(coordinator.devices) if coordinator else 0,
        },
        "device_registry_count": len(
            dr.async_entries_for_config_entry(
                dr.async_get(hass),
                entry.entry_id,
            )
        ),
        "entity_registry_count": len(
            er.async_entries_for_config_entry(
                er.async_get(hass),
                entry.entry_id,
            )
        ),
    }

    # Belt-and-braces: async_redact_data only catches secrets that
    # appear as a DICT KEY named e.g. "hub_key". A secret embedded
    # inside a VALUE (an error message, a sanitized-but-imperfect
    # exception string) would slip past that. scrub_deep() walks every
    # string in the whole payload and removes anything matching a
    # known secret value or credential pattern, using the sanitize
    # module already used by api.py/coordinator.py.
    return scrub_deep(result, secrets)
