"""Diagnostic sensors for SkylinkNet."""

from __future__ import annotations

from datetime import datetime

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import SkylinkNetCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up SkylinkNet diagnostic sensors."""

    data = hass.data[DOMAIN][entry.entry_id]

    coordinator: SkylinkNetCoordinator = data["coordinator"]

    async_add_entities(
        [
            SkylinkNetConnectionCount(coordinator),
            SkylinkNetReconnectCount(coordinator),
            SkylinkNetMessageCount(coordinator),
            SkylinkNetLastMessage(coordinator),
        ]
    )


class SkylinkNetDiagnosticSensor(SensorEntity):
    """Base SkylinkNet diagnostic sensor."""

    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(
        self,
        coordinator: SkylinkNetCoordinator,
    ) -> None:
        """Initialize."""

        self.coordinator = coordinator

        self._attr_device_info = (
            coordinator.hub_device_info
        )

        coordinator.add_monitor_listener(
            self._monitor_changed
        )

    def _monitor_changed(self) -> None:
        """Handle coordinator monitoring update."""

        self.async_write_ha_state()

    async def async_will_remove_from_hass(self) -> None:
        """Remove listener."""

        self.coordinator.remove_monitor_listener(
            self._monitor_changed
        )

        await super().async_will_remove_from_hass()


class SkylinkNetConnectionCount(
    SkylinkNetDiagnosticSensor
):
    """WebSocket connection count."""

    _attr_name = "WebSocket Connections"
    _attr_icon = "mdi:connection"
    _attr_state_class = SensorStateClass.TOTAL_INCREASING

    def __init__(
        self,
        coordinator: SkylinkNetCoordinator,
    ) -> None:
        super().__init__(coordinator)

        self._attr_unique_id = (
            f"skylinknet_{coordinator.api.hub_id}"
            "_websocket_connections"
        )

    @property
    def native_value(self) -> int:
        """Return connection count."""

        return self.coordinator.connection_count


class SkylinkNetReconnectCount(
    SkylinkNetDiagnosticSensor
):
    """WebSocket reconnect count."""

    _attr_name = "WebSocket Reconnects"
    _attr_icon = "mdi:refresh"
    _attr_state_class = SensorStateClass.TOTAL_INCREASING

    def __init__(
        self,
        coordinator: SkylinkNetCoordinator,
    ) -> None:
        super().__init__(coordinator)

        self._attr_unique_id = (
            f"skylinknet_{coordinator.api.hub_id}"
            "_websocket_reconnects"
        )

    @property
    def native_value(self) -> int:
        """Return reconnect count."""

        return self.coordinator.reconnect_count


class SkylinkNetMessageCount(
    SkylinkNetDiagnosticSensor
):
    """WebSocket message count."""

    _attr_name = "WebSocket Messages"
    _attr_icon = "mdi:message-processing"
    _attr_state_class = SensorStateClass.TOTAL_INCREASING

    def __init__(
        self,
        coordinator: SkylinkNetCoordinator,
    ) -> None:
        super().__init__(coordinator)

        self._attr_unique_id = (
            f"skylinknet_{coordinator.api.hub_id}"
            "_websocket_messages"
        )

    @property
    def native_value(self) -> int:
        """Return message count."""

        return self.coordinator.message_count


class SkylinkNetLastMessage(
    SkylinkNetDiagnosticSensor
):
    """Last WebSocket message timestamp."""

    _attr_name = "WebSocket Last Message"
    _attr_icon = "mdi:clock-outline"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(
        self,
        coordinator: SkylinkNetCoordinator,
    ) -> None:
        super().__init__(coordinator)

        self._attr_unique_id = (
            f"skylinknet_{coordinator.api.hub_id}"
            "_websocket_last_message"
        )

    @property
    def native_value(self) -> datetime | None:
        """Return last message timestamp."""

        return self.coordinator.last_message
