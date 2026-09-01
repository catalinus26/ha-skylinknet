"""SkylinkNet coordinator."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import ssl
from datetime import datetime
from typing import Any

import aiohttp

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .api import SkylinkNetApi
from .const import (
    ALARM_CODE_ARMED_AWAY,
    ALARM_CODE_ARMED_HOME,
    ALARM_CODE_DISARMED,
    API_URL,
    DOMAIN,
    STORAGE_VERSION,
    WEBSOCKET_ENDPOINT,
)

_LOGGER = logging.getLogger(__name__)


# ============================================================
# WEBSOCKET SETTINGS
# ============================================================

INITIAL_RECONNECT_DELAY = 5
MAX_RECONNECT_DELAY = 300
WS_HEARTBEAT = 30


# ============================================================
# VIRTUAL ALARM DEVICE
# ============================================================

ALARM_DEVICE_ID = "F0000000"


# ============================================================
# ALARM STATUS VALUES
# ============================================================

ALARM_STATUS_ARM_AWAY_START = 7
ALARM_STATUS_ARMED_AWAY = ALARM_CODE_ARMED_AWAY
ALARM_STATUS_DISARMED = ALARM_CODE_DISARMED
ALARM_STATUS_TRIGGERED_OPEN_ZONE = 5
ALARM_STATUS_TRIGGERED = 6


class SkylinkNetCoordinator:
    """Handle SkylinkNet API and WebSocket communication."""

    # Expose constant to other integration modules.
    ALARM_DEVICE_ID = ALARM_DEVICE_ID

    def __init__(
        self,
        hass: HomeAssistant,
        api: SkylinkNetApi,
        entry_id: str,
    ) -> None:
        """Initialize coordinator."""

        self.hass = hass
        self.api = api

        self._task: asyncio.Task | None = None
        self._stop = False

        self._ssl_context: ssl.SSLContext | None = None

        self.devices: dict[str, dict[str, Any]] = {}
        self.states: dict[str, dict[str, Any]] = {}

        self._listeners: list = []
        self._monitor_listeners: list = []

        # ========================================================
        # PERSISTENCE / DYNAMIC DISCOVERY
        # ========================================================

        self._store: Store = Store(
            hass,
            STORAGE_VERSION,
            f"{DOMAIN}.{entry_id}",
        )

        self.known_device_ids: set[str] = set()
        self.ignored_device_ids: set[str] = set()

        self._new_device_listeners: list = []

        # ========================================================
        # WEBSOCKET MONITORING
        # ========================================================

        self.websocket_connected = False

        self.websocket_connect_count = 0
        self.websocket_disconnect_count = 0
        self.websocket_reconnect_count = 0
        self.websocket_message_count = 0
        self.websocket_error_count = 0

        self._had_first_connection = False

        self.websocket_last_connect: datetime | None = None
        self.websocket_last_disconnect: datetime | None = None
        self.websocket_last_message: datetime | None = None

        self.websocket_last_error: str | None = None

        self.websocket_reconnect_delay = (
            INITIAL_RECONNECT_DELAY
        )

        # ========================================================
        # ALARM
        # ========================================================

        self.alarm_state = "disarmed"

        self._arming_mode = "disarmed"

        self._exit_delay = False

        self._entry_delay = False

    # ============================================================
    # START
    # ============================================================

    async def start(self) -> None:
        """Start coordinator."""

        if (
            self._task is not None
            and not self._task.done()
        ):
            return

        self._stop = False

        self._ssl_context = (
            await self.hass.async_add_executor_job(
                ssl.create_default_context
            )
        )

        self._task = (
            self.hass.async_create_background_task(
                self._websocket_loop(),
                name="skylinknet_websocket",
            )
        )

    # ============================================================
    # STOP
    # ============================================================

    async def stop(self) -> None:
        """Stop coordinator."""

        self._stop = True

        if self._task is not None:
            self._task.cancel()

            try:
                await self._task
            except asyncio.CancelledError:
                pass

            self._task = None

        self._set_websocket_connected(False)

    # ============================================================
    # LISTENERS
    # ============================================================

    def add_listener(self, callback) -> None:
        """Add state listener."""

        if callback not in self._listeners:
            self._listeners.append(callback)

    def remove_listener(self, callback) -> None:
        """Remove state listener."""

        if callback in self._listeners:
            self._listeners.remove(callback)

    def add_monitor_listener(self, callback) -> None:
        """Add WebSocket monitor listener."""

        if callback not in self._monitor_listeners:
            self._monitor_listeners.append(callback)

    def remove_monitor_listener(self, callback) -> None:
        """Remove WebSocket monitor listener."""

        if callback in self._monitor_listeners:
            self._monitor_listeners.remove(callback)

    def add_new_device_listener(self, callback) -> None:
        """Add listener for new device discovery."""

        if callback not in self._new_device_listeners:
            self._new_device_listeners.append(callback)

    def remove_new_device_listener(self, callback) -> None:
        """Remove new-device listener."""

        if callback in self._new_device_listeners:
            self._new_device_listeners.remove(callback)

    def _notify_new_device_listeners(
        self,
        dev_id: str,
    ) -> None:
        """Notify listeners about new device."""

        for callback in list(
            self._new_device_listeners
        ):
            try:
                callback(dev_id)
            except Exception:
                _LOGGER.exception(
                    "SkylinkNet new-device listener error"
                )

    def _notify_listeners(
        self,
        dev_id: str,
    ) -> None:
        """Notify sensor listeners."""

        for callback in list(
            self._listeners
        ):
            try:
                callback(dev_id)
            except Exception:
                _LOGGER.exception(
                    "SkylinkNet sensor listener error"
                )

    def _notify_monitor_listeners(self) -> None:
        """Notify monitor listeners."""

        for callback in list(
            self._monitor_listeners
        ):
            try:
                callback()
            except Exception:
                _LOGGER.exception(
                    "SkylinkNet monitor listener error"
                )

    # ============================================================
    # PERSISTENCE
    # ============================================================

    async def async_load_persisted(self) -> None:
        """Load persistent device information."""

        stored = (
            await self._store.async_load()
            or {}
        )

        self.known_device_ids = set(
            stored.get(
                "device_ids",
                [],
            )
        )

        self.ignored_device_ids = set(
            stored.get(
                "ignored_device_ids",
                [],
            )
        )

        for dev_id, item in stored.get(
            "device_states",
            {},
        ).items():
            if isinstance(item, dict):
                self.states[dev_id] = dict(item)

                self.known_device_ids.add(
                    dev_id
                )

                if dev_id not in self.devices:
                    self.devices[dev_id] = {
                        "dev_id": dev_id,
                    }

        # Remove ignored devices from known state.
        for dev_id in self.ignored_device_ids:
            self.known_device_ids.discard(
                dev_id
            )

            self.states.pop(
                dev_id,
                None,
            )

            self.devices.pop(
                dev_id,
                None,
            )

    async def async_save_persisted(self) -> None:
        """Persist known devices and states."""

        device_states = {
            dev_id: self.states[dev_id]
            for dev_id in self.known_device_ids
            if dev_id in self.states
        }

        await self._store.async_save(
            {
                "device_ids": sorted(
                    self.known_device_ids
                ),
                "ignored_device_ids": sorted(
                    self.ignored_device_ids
                ),
                "device_states": device_states,
            }
        )

    # ============================================================
    # FORGET / ALLOW DEVICE
    # ============================================================

    async def async_forget_device(
        self,
        dev_id: str,
        ignore_future: bool = True,
    ) -> bool:
        """Forget a device."""

        matched = (
            dev_id in self.known_device_ids
            or dev_id in self.devices
            or dev_id in self.states
        )

        if not matched:
            return False

        self.known_device_ids.discard(
            dev_id
        )

        self.devices.pop(
            dev_id,
            None,
        )

        self.states.pop(
            dev_id,
            None,
        )

        if ignore_future:
            self.ignored_device_ids.add(
                dev_id
            )

        await self.async_save_persisted()

        return True

    async def async_allow_device(
        self,
        dev_id: str,
    ) -> bool:
        """Allow previously ignored device."""

        if dev_id not in self.ignored_device_ids:
            return False

        self.ignored_device_ids.discard(
            dev_id
        )

        await self.async_save_persisted()

        return True

    # ============================================================
    # WEBSOCKET STATUS
    # ============================================================

    def _set_websocket_connected(
        self,
        connected: bool,
    ) -> None:
        """Set WebSocket connection state."""

        if (
            self.websocket_connected
            == connected
        ):
            return

        self.websocket_connected = connected

        now = dt_util.utcnow()

        if connected:
            self.websocket_connect_count += 1

            self.websocket_last_connect = now

            if self._had_first_connection:
                self.websocket_reconnect_count += 1

            self._had_first_connection = True

        else:
            self.websocket_disconnect_count += 1

            self.websocket_last_disconnect = now

        self._notify_monitor_listeners()

    # ============================================================
    # WEBSOCKET URL
    # ============================================================

    @property
    def websocket_url(self) -> str:
        """Return WebSocket URL."""

        ws_base = API_URL.replace(
            "https://",
            "wss://",
            1,
        )

        return ws_base + WEBSOCKET_ENDPOINT.format(
            hub_id=self.api.hub_id,
            hub_key=self.api.hub_key,
        )

    # ============================================================
    # WEBSOCKET LOOP
    # ============================================================

    async def _websocket_loop(self) -> None:
        """Maintain WebSocket connection."""

        if self._ssl_context is None:
            _LOGGER.error(
                "SkylinkNet SSL context is not initialized"
            )
            return

        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=30,
            sock_connect=30,
            sock_read=None,
        )

        reconnect_delay = (
            INITIAL_RECONNECT_DELAY
        )

        while not self._stop:

            try:
                _LOGGER.info(
                    "SkylinkNet WebSocket connecting..."
                )

                async with aiohttp.ClientSession(
                    timeout=timeout
                ) as session:

                    async with session.ws_connect(
                        self.websocket_url,
                        ssl=self._ssl_context,
                        heartbeat=WS_HEARTBEAT,
                        autoclose=True,
                        autoping=True,
                    ) as ws:

                        self._set_websocket_connected(
                            True
                        )

                        reconnect_delay = (
                            INITIAL_RECONNECT_DELAY
                        )

                        self.websocket_last_error = (
                            None
                        )

                        _LOGGER.info(
                            "SkylinkNet WebSocket CONNECTED"
                        )

                        async for message in ws:

                            if self._stop:
                                break

                            self.websocket_message_count += 1

                            self.websocket_last_message = (
                                dt_util.utcnow()
                            )

                            if (
                                message.type
                                == aiohttp.WSMsgType.TEXT
                            ):
                                self._process_message(
                                    message.data
                                )

                            elif (
                                message.type
                                == aiohttp.WSMsgType.BINARY
                            ):
                                try:
                                    text = (
                                        message.data.decode(
                                            "utf-8"
                                        )
                                    )

                                    self._process_message(
                                        text
                                    )

                                except Exception:
                                    _LOGGER.debug(
                                        "SkylinkNet binary message"
                                    )

                            elif (
                                message.type
                                == aiohttp.WSMsgType.PING
                            ):
                                _LOGGER.debug(
                                    "SkylinkNet WebSocket PING"
                                )

                            elif (
                                message.type
                                == aiohttp.WSMsgType.PONG
                            ):
                                _LOGGER.debug(
                                    "SkylinkNet WebSocket PONG"
                                )

                            elif (
                                message.type
                                == aiohttp.WSMsgType.ERROR
                            ):
                                self.websocket_error_count += 1

                                error = ws.exception()

                                self.websocket_last_error = (
                                    str(error)
                                )

                                _LOGGER.error(
                                    "SkylinkNet WebSocket error: %s",
                                    error,
                                )

                                break

                            elif message.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                _LOGGER.warning(
                                    "SkylinkNet WebSocket closed"
                                )
                                break

            except asyncio.CancelledError:
                self._set_websocket_connected(
                    False
                )
                raise

            except Exception as err:
                self.websocket_error_count += 1

                self.websocket_last_error = str(
                    err
                )

                _LOGGER.exception(
                    "SkylinkNet WebSocket connection failed"
                )

            finally:
                self._set_websocket_connected(
                    False
                )

            if self._stop:
                break

            sleep_for = (
                reconnect_delay
                + random.uniform(
                    -0.2 * reconnect_delay,
                    0.2 * reconnect_delay,
                )
            )

            sleep_for = max(
                1,
                sleep_for,
            )

            _LOGGER.warning(
                "SkylinkNet WebSocket reconnect in %.1f seconds",
                sleep_for,
            )

            try:
                await asyncio.sleep(
                    sleep_for
                )
            except asyncio.CancelledError:
                raise

            reconnect_delay = min(
                reconnect_delay * 2,
                MAX_RECONNECT_DELAY,
            )

            self.websocket_reconnect_delay = (
                reconnect_delay
            )

    # ============================================================
    # PROCESS MESSAGE
    # ============================================================

    def _process_message(
        self,
        message: str,
    ) -> None:
        """Process WebSocket message."""

        _LOGGER.debug(
            "SkylinkNet WebSocket RX: %s",
            message,
        )

        try:
            data = json.loads(message)

        except json.JSONDecodeError:
            _LOGGER.debug(
                "SkylinkNet non-JSON message: %s",
                message,
            )
            return

        if not isinstance(data, dict):
            return

        op = data.get("op")
        hub_id = data.get("hub_id")
        payload = data.get("data")

        _LOGGER.debug(
            "SkylinkNet WS message op=%s hub_id=%s",
            op,
            hub_id,
        )

        # Alarm virtual device.
        self._process_alarm_message(data)

        # Device list.
        if isinstance(payload, list):

            for item in payload:

                if not isinstance(item, dict):
                    continue

                self._ingest_device_item(item)

            return

        # Single device.
        if isinstance(payload, dict):

            self._ingest_device_item(
                payload
            )

    # ============================================================
    # DEVICE MESSAGE
    # ============================================================

    def _ingest_device_item(
        self,
        item: dict[str, Any],
    ) -> None:
        """Ingest device state from WebSocket."""

        dev_id = item.get("dev_id")

        if not dev_id:
            return

        # Virtual alarm device is handled separately.
        if dev_id == ALARM_DEVICE_ID:
            return

        # Ignore forgotten devices.
        if dev_id in self.ignored_device_ids:
            _LOGGER.debug(
                "SkylinkNet ignoring event for "
                "forgotten device %s",
                dev_id,
            )
            return

        is_new = (
            dev_id
            not in self.known_device_ids
        )

        old_state = self.states.get(
            dev_id
        )

        self.states[dev_id] = dict(
            item
        )

        if dev_id not in self.devices:
            self.devices[dev_id] = {
                "dev_id": dev_id,
            }

        if is_new:
            self.known_device_ids.add(
                dev_id
            )

            _LOGGER.info(
                "SkylinkNet discovered new device: %s",
                dev_id,
            )

            self.hass.async_create_task(
                self.async_save_persisted()
            )

            self._notify_new_device_listeners(
                dev_id
            )

        elif old_state != self.states[
            dev_id
        ]:
            self.hass.async_create_task(
                self.async_save_persisted()
            )

        if old_state != self.states[
            dev_id
        ]:
            self._notify_listeners(
                dev_id
            )

    # ============================================================
    # ALARM MESSAGE
    # ============================================================

    def _process_alarm_message(
        self,
        data: dict[str, Any],
    ) -> None:
        """Process virtual SkylinkNet alarm device."""

        payload = data.get("data")

        items: list[dict[str, Any]] = []

        items.append(data)

        if isinstance(
            payload,
            dict,
        ):
            items.append(payload)

        elif isinstance(
            payload,
            list,
        ):
            items.extend(
                item
                for item in payload
                if isinstance(
                    item,
                    dict,
                )
            )

        for item in items:

            dev_id = item.get(
                "dev_id"
            )

            if dev_id != ALARM_DEVICE_ID:
                continue

            value = item.get(
                "status"
            )

            if value is None:
                continue

            try:
                status = int(value)
            except (
                TypeError,
                ValueError,
            ):
                continue

            self._handle_alarm_status(
                status
            )

            return

    # ============================================================
    # ALARM STATUS HANDLER
    # ============================================================

    def _handle_alarm_status(
        self,
        status: int,
    ) -> None:
        """Handle live WebSocket alarm status."""

        _LOGGER.debug(
            "SkylinkNet alarm status=%s arming_mode=%s",
            status,
            self._arming_mode,
        )

        # ========================================================
        # DISARM
        # ========================================================

        if status == ALARM_STATUS_DISARMED:

            self._arming_mode = "disarmed"

            self._exit_delay = False
            self._entry_delay = False

            self.alarm_state = "disarmed"

            self._notify_monitor_listeners()

            return

        # ========================================================
        # ARM HOME
        # ========================================================

        if status == ALARM_CODE_ARMED_HOME:

            self._arming_mode = "armed_home"

            self._exit_delay = False
            self._entry_delay = False

            self.alarm_state = "armed_home"

            self._notify_monitor_listeners()

            return

        # ========================================================
        # ARM AWAY START
        # ========================================================

        if status == ALARM_STATUS_ARM_AWAY_START:

            self._arming_mode = "armed_away"

            self._exit_delay = True
            self._entry_delay = False

            self.alarm_state = "arming"

            self._notify_monitor_listeners()

            return

        # ========================================================
        # STATUS 3
        #
        # 3 = stable Armed Away
        #
        # When received after status=7, it means the exit delay
        # has completed.
        #
        # When received while already armed, it may represent
        # Entry Delay.
        # ========================================================

        if status == ALARM_STATUS_ARMED_AWAY:

            if self._arming_mode == "armed_away":

                if self._exit_delay:

                    self._exit_delay = False
                    self._entry_delay = False

                    self.alarm_state = (
                        "armed_away"
                    )

                else:

                    self._entry_delay = True

                    self.alarm_state = (
                        "pending"
                    )

            else:

                self._arming_mode = (
                    "armed_away"
                )

                self._exit_delay = False
                self._entry_delay = False

                self.alarm_state = (
                    "armed_away"
                )

            self._notify_monitor_listeners()

            return

        # ========================================================
        # TRIGGERED BY OPEN ZONE
        # ========================================================

        if status == ALARM_STATUS_TRIGGERED_OPEN_ZONE:

            if self._arming_mode not in (
                "armed_home",
                "armed_away",
            ):
                _LOGGER.debug(
                    "SkylinkNet open-zone trigger ignored "
                    "while disarmed"
                )
                return

            self._exit_delay = False
            self._entry_delay = False

            self.alarm_state = "triggered"

            self._notify_monitor_listeners()

            return

        # ========================================================
        # TRIGGERED
        # ========================================================

        if status == ALARM_STATUS_TRIGGERED:

            self._exit_delay = False
            self._entry_delay = False

            self.alarm_state = "triggered"

            self._notify_monitor_listeners()

            return

        _LOGGER.debug(
            "SkylinkNet unknown alarm status: %s",
            status,
        )

    # ============================================================
    # INITIAL ALARM STATE FROM READ
    # ============================================================

    def update_alarm_state_from_read(
        self,
        read: Any,
    ) -> None:
        """Restore alarm state from initial read response.

        The SkylinkNet REST get_hub_status response does not contain
        the alarm state.

        The read response does contain the virtual alarm device:

            F0000000 status=4 -> disarmed
            F0000000 status=3 -> armed away

        This method is called once during integration startup,
        before entities are created.
        """

        if not isinstance(
            read,
            dict,
        ):
            _LOGGER.warning(
                "SkylinkNet initial read is not a dictionary"
            )
            return

        data = read.get(
            "data"
        )

        if not isinstance(
            data,
            list,
        ):
            _LOGGER.warning(
                "SkylinkNet initial read has no data list"
            )
            return

        for item in data:

            if not isinstance(
                item,
                dict,
            ):
                continue

            dev_id = item.get(
                "dev_id"
            )

            if dev_id != ALARM_DEVICE_ID:
                continue

            value = item.get(
                "status"
            )

            if value is None:
                _LOGGER.warning(
                    "SkylinkNet initial alarm device "
                    "has no status"
                )
                return

            try:
                status = int(value)
            except (
                TypeError,
                ValueError,
            ):
                _LOGGER.warning(
                    "SkylinkNet invalid initial alarm status: %s",
                    value,
                )
                return

            _LOGGER.debug(
                "SkylinkNet initial alarm status: %s",
                status,
            )

            # ----------------------------------------------------
            # DISARMED
            # ----------------------------------------------------

            if status == ALARM_STATUS_DISARMED:

                self._arming_mode = "disarmed"

                self._exit_delay = False
                self._entry_delay = False

                self.alarm_state = "disarmed"

            # ----------------------------------------------------
            # ARMED HOME
            # ----------------------------------------------------

            elif status == ALARM_CODE_ARMED_HOME:

                self._arming_mode = "armed_home"

                self._exit_delay = False
                self._entry_delay = False

                self.alarm_state = "armed_home"

            # ----------------------------------------------------
            # ARMED AWAY
            # ----------------------------------------------------

            elif status == ALARM_STATUS_ARMED_AWAY:

                self._arming_mode = "armed_away"

                self._exit_delay = False
                self._entry_delay = False

                self.alarm_state = "armed_away"

            # ----------------------------------------------------
            # UNKNOWN
            # ----------------------------------------------------

            else:

                _LOGGER.warning(
                    "SkylinkNet unknown initial alarm status: %s",
                    status,
                )

                return

            _LOGGER.info(
                "SkylinkNet initial alarm state restored: "
                "status=%s state=%s",
                status,
                self.alarm_state,
            )

            self._notify_monitor_listeners()

            return

        _LOGGER.warning(
            "SkylinkNet initial read does not contain "
            "alarm device %s",
            ALARM_DEVICE_ID,
        )

    # ============================================================
    # ALARM STATE NORMALIZATION
    # ============================================================

    @staticmethod
    def _normalize_alarm_state(
        value: Any,
    ) -> str | None:
        """Normalize explicit SkylinkNet alarm state."""

        if isinstance(
            value,
            str,
        ):

            value = value.lower().strip()

            mapping = {
                "disarm": "disarmed",
                "disarmed": "disarmed",
                "arm_home": "armed_home",
                "armed_home": "armed_home",
                "home": "armed_home",
                "arm_away": "armed_away",
                "armed_away": "armed_away",
                "away": "armed_away",
                "triggered": "triggered",
            }

            return mapping.get(
                value
            )

        try:
            value = int(value)
        except (
            TypeError,
            ValueError,
        ):
            return None

        mapping = {
            ALARM_CODE_ARMED_HOME: "armed_home",
            ALARM_CODE_ARMED_AWAY: "armed_away",
            ALARM_CODE_DISARMED: "disarmed",
        }

        return mapping.get(
            value
        )

    # ============================================================
    # LEGACY / HUB STATUS
    # ============================================================

    def update_alarm_state_from_status(
        self,
        status: Any,
    ) -> None:
        """Update alarm state from hub status.

        Kept for compatibility, but get_hub_status currently does
        not expose the alarm state. Startup therefore uses
        update_alarm_state_from_read() instead.
        """

        if not isinstance(
            status,
            dict,
        ):
            return

        candidates = [
            status,
            status.get("data"),
        ]

        for item in candidates:

            if not isinstance(
                item,
                dict,
            ):
                continue

            for key in (
                "alarm",
                "alarm_status",
                "alarmState",
                "alarm_state",
                "status",
            ):
                if key not in item:
                    continue

                value = item[key]

                state = (
                    self._normalize_alarm_state(
                        value
                    )
                )

                if state is None:
                    continue

                self.alarm_state = state

                if state == "armed_home":
                    self._arming_mode = (
                        "armed_home"
                    )

                elif state == "armed_away":
                    self._arming_mode = (
                        "armed_away"
                    )

                elif state == "disarmed":
                    self._arming_mode = (
                        "disarmed"
                    )

                return

    # ============================================================
    # DEVICE INFO
    # ============================================================

    @property
    def hub_device_info(
        self,
    ) -> dict[str, Any]:
        """Return device info for SkylinkNet hub."""

        return {
            "identifiers": {
                (
                    DOMAIN,
                    str(self.api.hub_id),
                )
            },
            "name": (
                f"SkylinkNet Hub "
                f"{self.api.hub_id}"
            ),
            "manufacturer": "SkylinkNet",
        }

    def device_info_for(
        self,
        dev_id: str,
    ) -> dict[str, Any]:
        """Return device info for physical sensor."""

        device = (
            self.get_device(dev_id)
            or {}
        )

        return {
            "identifiers": {
                (
                    DOMAIN,
                    dev_id,
                )
            },
            "name": device.get(
                "dev_name",
                dev_id,
            ),
            "via_device": (
                DOMAIN,
                str(self.api.hub_id),
            ),
        }

    # ============================================================
    # DEVICE HELPERS
    # ============================================================

    def get_device(
        self,
        dev_id: str,
    ) -> dict[str, Any] | None:
        """Return device configuration."""

        return self.devices.get(
            dev_id
        )

    def get_state(
        self,
        dev_id: str,
    ) -> dict[str, Any]:
        """Return current state."""

        return self.states.get(
            dev_id,
            {},
        )

    def get_status(
        self,
        dev_id: str,
    ) -> int:
        """Return device status."""

        state = self.get_state(
            dev_id
        )

        try:
            return int(
                state.get(
                    "status",
                    0,
                )
            )
        except (
            TypeError,
            ValueError,
        ):
            return 0

    def is_open(
        self,
        dev_id: str,
    ) -> bool:
        """Return True when sensor is active/open."""

        return (
            self.get_status(dev_id)
            == 1
        )

    def get_battery(
        self,
        dev_id: str,
    ) -> int | None:
        """Return battery status."""

        state = self.get_state(
            dev_id
        )

        battery = state.get(
            "battery"
        )

        if battery is None:
            return None

        try:
            return int(
                battery
            )
        except (
            TypeError,
            ValueError,
        ):
            return None

    # ============================================================
    # WEBSOCKET MONITORING
    # ============================================================

    @property
    def connection_count(
        self,
    ) -> int:
        """Return successful WebSocket connections."""

        return self.websocket_connect_count

    @property
    def reconnect_count(
        self,
    ) -> int:
        """Return WebSocket reconnections."""

        return self.websocket_reconnect_count

    @property
    def message_count(
        self,
    ) -> int:
        """Return WebSocket messages received."""

        return self.websocket_message_count

    @property
    def last_message(
        self,
    ) -> datetime | None:
        """Return last message timestamp."""

        return self.websocket_last_message

    def get_websocket_info(
        self,
    ) -> dict[str, Any]:
        """Return WebSocket diagnostics."""

        return {
            "connected": self.websocket_connected,
            "connect_count": (
                self.websocket_connect_count
            ),
            "disconnect_count": (
                self.websocket_disconnect_count
            ),
            "message_count": (
                self.websocket_message_count
            ),
            "error_count": (
                self.websocket_error_count
            ),
            "last_error": (
                self.websocket_last_error
            ),
            "reconnect_delay": (
                self.websocket_reconnect_delay
            ),
        }

    # ============================================================
    # ALARM COMMAND
    # ============================================================

    async def set_alarm(
        self,
        alarm: str,
        bypass: str | None = None,
    ) -> bool:
        """Set SkylinkNet alarm state."""

        try:
            result = await self.api.set_alarm(
                alarm,
                bypass=bypass,
            )

        except Exception as err:
            _LOGGER.error(
                "SkylinkNet alarm command failed: %s",
                err,
            )
            return False

        if not isinstance(
            result,
            dict,
        ):
            _LOGGER.error(
                "SkylinkNet alarm returned invalid response: %s",
                result,
            )
            return False

        try:
            errno = int(
                result.get(
                    "errno",
                    0,
                )
            )
        except (
            TypeError,
            ValueError,
        ):
            errno = -1

        if errno != 0:
            _LOGGER.error(
                "SkylinkNet alarm command failed: %s",
                result,
            )
            return False

        arming_mode = {
            "disarm": "disarmed",
            "arm_home": "armed_home",
            "arm_away": "armed_away",
        }.get(alarm)

        if arming_mode:

            self._arming_mode = (
                arming_mode
            )

            self._exit_delay = (
                alarm == "arm_away"
            )

            self._entry_delay = False

            self.alarm_state = (
                "arming"
                if self._exit_delay
                else arming_mode
            )

            self._notify_monitor_listeners()

        _LOGGER.info(
            "SkylinkNet alarm command successful: "
            "alarm=%s bypass=%s",
            alarm,
            bypass,
        )

        return True