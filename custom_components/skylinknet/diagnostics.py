"""Diagnostics for SkylinkNet."""

from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er

from .const import DOMAIN

TO_REDACT = {"email", "password", "hub_id", "hub_key"}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    """Return diagnostics for a SkylinkNet config entry."""

    data = hass.data.get(DOMAIN, {}).get(entry.entry_id)

    coordinator = data.get("coordinator") if data else None

    return {
        "entry": {
            "data": _redact(entry.data),
            "options": dict(entry.options),
        },
        "runtime": {
            "websocket_connected": (
                coordinator.websocket_connected if coordinator else None
            ),
            "websocket_connect_count": (
                coordinator.websocket_connect_count if coordinator else None
            ),
            "websocket_disconnect_count": (
                coordinator.websocket_disconnect_count if coordinator else None
            ),
            "websocket_reconnect_count": (
                coordinator.websocket_reconnect_count if coordinator else None
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


def _redact(data: dict[str, Any]) -> dict[str, Any]:
    """Redact sensitive fields (email/password/hub credentials)."""

    redacted = dict(data)

    for key in TO_REDACT:
        if key in redacted:
            redacted[key] = "**REDACTED**"

    return redacted
