
"""Alarm control panel for SkylinkNet."""

from __future__ import annotations

import logging

from homeassistant.components.alarm_control_panel import (
    AlarmControlPanelEntity,
    AlarmControlPanelEntityFeature,
    AlarmControlPanelState,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import SkylinkNetCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up SkylinkNet alarm control panel."""

    data = hass.data[DOMAIN][entry.entry_id]

    coordinator: SkylinkNetCoordinator = data["coordinator"]

    async_add_entities(
        [
            SkylinkNetAlarmControlPanel(coordinator)
        ]
    )


class SkylinkNetAlarmControlPanel(AlarmControlPanelEntity):
    """SkylinkNet alarm."""

    _attr_has_entity_name = True
    _attr_name = "Alarm"
    _attr_should_poll = False

    # SkylinkNet does not require a PIN from HA.
    _attr_code_arm_required = False
    _attr_code_disarm_required = False

    # Explicitly advertise supported arming modes.
    #
    # NOTE: there is no AlarmControlPanelEntityFeature.DISARM.
    # Disarm is always available by default and must not be
    # listed here — referencing it raises AttributeError at
    # import time and breaks the whole platform.
    _attr_supported_features = (
        AlarmControlPanelEntityFeature.ARM_HOME
        | AlarmControlPanelEntityFeature.ARM_AWAY
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
            "_alarm"
        )

        self._attr_device_info = (
            coordinator.hub_device_info
        )

        coordinator.add_monitor_listener(
            self._state_changed
        )

    # ============================================================
    # STATE
    # ============================================================

    # Coordinator internal state -> HA display state.
    _STATE_MAP = {
        "disarmed": AlarmControlPanelState.DISARMED,
        "armed_home": AlarmControlPanelState.ARMED_HOME,
        "armed_away": AlarmControlPanelState.ARMED_AWAY,
        # Exit delay (arm away in progress).
        "arming": AlarmControlPanelState.ARMING,
        # Entry delay (sensor tripped while armed, not yet
        # confirmed as a real trigger).
        "pending": AlarmControlPanelState.PENDING,
        "triggered": AlarmControlPanelState.TRIGGERED,
    }

    @property
    def alarm_state(self) -> AlarmControlPanelState:
        """Return current alarm state."""

        state = self.coordinator.alarm_state

        return self._STATE_MAP.get(
            state,
            AlarmControlPanelState.DISARMED,
        )

    # ============================================================
    # DISARM
    # ============================================================

    async def async_alarm_disarm(
        self,
        code: str | None = None,
    ) -> None:
        """Disarm alarm."""

        _LOGGER.info(
            "SkylinkNet DISARM requested from Home Assistant"
        )

        success = await self.coordinator.set_alarm(
            "disarm"
        )

        if success:
            self.async_write_ha_state()

    # ============================================================
    # ARM HOME
    # ============================================================

    async def async_alarm_arm_home(
        self,
        *args,
        **kwargs,
    ) -> None:
        """Arm home."""

        _LOGGER.info(
            "SkylinkNet ARM HOME requested from Home Assistant"
        )

        success = await self.coordinator.set_alarm(
            "arm_home",
            bypass="1",
        )

        if success:
            self.async_write_ha_state()

    # ============================================================
    # ARM AWAY
    # ============================================================

    async def async_alarm_arm_away(
        self,
        *args,
        **kwargs,
    ) -> None:
        """Arm away."""

        _LOGGER.info(
            "SkylinkNet ARM AWAY requested from Home Assistant"
        )

        success = await self.coordinator.set_alarm(
            "arm_away",
            bypass="1",
        )

        if success:
            self.async_write_ha_state()

    # ============================================================
    # STATE UPDATE
    # ============================================================

    def _state_changed(self) -> None:
        """Handle coordinator update."""

        self.async_write_ha_state()

    # ============================================================
    # REMOVE
    # ============================================================

    async def async_will_remove_from_hass(
        self,
    ) -> None:
        """Remove listener."""

        self.coordinator.remove_monitor_listener(
            self._state_changed
        )

        await super().async_will_remove_from_hass()

