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
from .sanitize import sanitize_exception
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


def _iso(value: datetime | None) -> str | None:
    """Render a datetime as ISO-8601, or None, for diagnostics output."""

    return value.isoformat() if value else None


# ============================================================
# WEBSOCKET SETTINGS
# ============================================================

INITIAL_RECONNECT_DELAY = 5
MAX_RECONNECT_DELAY = 300
WS_HEARTBEAT = 30

# ============================================================
# ARM CONFIRMATION SAFETY NET
#
# When arming is requested (from Home Assistant OR from the
# SkylinkNet app / another client), the alarm briefly goes
# through a transient state ("arming" = exit delay, "pending" =
# entry delay) before the hub is expected to push a WebSocket
# confirmation for the virtual alarm device (F0000000) with the
# final status (2/3/4).
#
# In practice this confirmation does not always arrive over the
# WebSocket (e.g. depending on how the arm was triggered), which
# left the entity stuck showing "arming"/"pending" forever even
# though the alarm had actually finished arming. To guard against
# this, a watchdog is started whenever we enter a transient state;
# if no WebSocket confirmation clears it in time, we fall back to
# polling the REST API for the real status.
# ============================================================

ARM_CONFIRM_TIMEOUT = 60

# Extra, bounded retries of the REST fallback check when it fails
# (e.g. transient network error). This does NOT change how long it
# takes to fall back the FIRST time (still ARM_CONFIRM_TIMEOUT); it
# only stops a single failed REST call from leaving the entity stuck
# in "arming"/"pending" until the next full reconnect/resync. It is
# intentionally short and bounded so it can never become the
# "aggressive polling" the spec forbids. If every attempt fails, the
# alarm_state is left exactly as it was (arming/pending) — the
# watchdog never guesses a final state.
ARM_CONFIRM_RETRY_DELAYS = (15, 30)


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

# alarm_state values considered "transient": the watchdog and the
# resync logic treat these as "not yet confirmed".
TRANSIENT_ALARM_STATES = ("arming", "pending")


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

        # Safety-net watchdog for transient arm/entry-delay states
        # (see ARM_CONFIRM_TIMEOUT above).
        self._arm_confirm_task: asyncio.Task | None = None

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

        # ------------------------------------------------------
        # Finer-grained diagnostics (see get_websocket_info()).
        #
        # - "last_activity"    any inbound WS message (any type).
        # - "last_heartbeat"   a PING/PONG frame that actually
        #                      surfaced in the receive loop. With
        #                      aiohttp autoping=True, PING/PONG are
        #                      normally consumed internally and will
        #                      NOT appear here — this counter simply
        #                      stays at 0 in that case, which is
        #                      expected and not an error.
        # - "last_useful_message" a TEXT/BINARY message that could be
        #                      decoded as JSON (i.e. actually used).
        # - "invalid_message_count" TEXT/BINARY that failed to decode.
        # ------------------------------------------------------

        self.websocket_heartbeat_count = 0
        self.websocket_invalid_message_count = 0

        self.websocket_last_heartbeat: datetime | None = None
        self.websocket_last_useful_message: datetime | None = None

        self.websocket_last_error_kind: str | None = None

        self._had_first_connection = False

        self.websocket_last_connect: datetime | None = None
        self.websocket_last_disconnect: datetime | None = None
        self.websocket_last_message: datetime | None = None

        self.websocket_last_error: str | None = None

        self.websocket_reconnect_delay = (
            INITIAL_RECONNECT_DELAY
        )

        # ------------------------------------------------------
        # WebSocket -> REST resync (see _resync_after_connect()).
        # Single-flight: a lock, not a flag, so a resync already in
        # flight is simply awaited/skipped rather than started twice.
        # ------------------------------------------------------

        self._resync_lock = asyncio.Lock()

        # ========================================================
        # ALARM
        # ========================================================

        self.alarm_state = "disarmed"

        self._arming_mode = "disarmed"

        self._exit_delay = False

        self._entry_delay = False

        # Diagnostics for the last arm/disarm command (see
        # _note_arm_requested() / _note_arm_confirmed()).
        self.last_arm: dict[str, Any] | None = None

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

        self._cancel_arm_confirmation()

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
    # ARM CONFIRMATION SAFETY NET
    # ============================================================

    def _schedule_arm_confirmation(self) -> None:
        """(Re)start the watchdog while alarm_state is transient.

        Called every time we enter or re-enter "arming" (exit
        delay) or "pending" (entry delay). If the hub never sends
        a WebSocket confirmation for the virtual alarm device
        before ARM_CONFIRM_TIMEOUT elapses, the watchdog polls the
        REST API so the entity does not stay stuck indefinitely.
        """

        self._cancel_arm_confirmation()

        self._arm_confirm_task = (
            self.hass.async_create_background_task(
                self._arm_confirmation_watchdog(),
                name="skylinknet_arm_confirmation",
            )
        )

    def _cancel_arm_confirmation(self) -> None:
        """Cancel any pending arm-confirmation watchdog."""

        if (
            self._arm_confirm_task is not None
            and not self._arm_confirm_task.done()
        ):
            self._arm_confirm_task.cancel()

        self._arm_confirm_task = None

    # ============================================================
    # LAST ARM DIAGNOSTICS
    #
    # Tracks only what the protocol actually confirmed:
    #   command / requested_at / result (REST accepted or not) /
    #   confirmed_at / confirmed_via / final_state.
    # It never claims bypass was applied — bypass="1" is forwarded
    # to the API unchanged, but whether the hub honoured it is not
    # observable from these responses (see set_alarm()/api.py).
    # ============================================================

    def _note_arm_requested(
        self,
        command: str,
    ) -> None:
        """Start tracking a newly-requested arm/disarm command."""

        self.last_arm = {
            "command": command,
            "requested_at": dt_util.utcnow(),
            "result": None,
            "confirmed_at": None,
            "confirmed_via": None,
            "final_state": None,
        }

    def _note_arm_result(
        self,
        result: str,
    ) -> None:
        """Record whether the REST call itself was accepted."""

        if self.last_arm is not None:
            self.last_arm["result"] = result

    def _note_arm_confirmed(
        self,
        confirmed_via: str,
    ) -> None:
        """Record that alarm_state settled on a non-transient value.

        No-op if there is no pending command, or if this command was
        already confirmed once (a later unrelated status change must
        not overwrite the confirmation of the command it belongs to).
        """

        arm = self.last_arm

        if arm is None or arm.get("confirmed_at") is not None:
            return

        arm["confirmed_at"] = dt_util.utcnow()
        arm["confirmed_via"] = confirmed_via
        arm["final_state"] = self.alarm_state

    def _note_arm_confirmation_failed(
        self,
        error: str | None,
    ) -> None:
        """Record that the watchdog gave up without confirming."""

        arm = self.last_arm

        if arm is None or arm.get("confirmed_at") is not None:
            return

        arm["confirmation_failed"] = True
        arm["last_confirmation_error"] = error

    async def _arm_confirmation_watchdog(self) -> None:
        """Fall back to REST polling if no WS confirmation arrives.

        Behaviour vs. v0.0.2: a single failed REST check no longer
        leaves the entity stuck in "arming"/"pending" until the next
        reconnect — it is retried a small, bounded number of times
        (ARM_CONFIRM_RETRY_DELAYS), a few seconds apart. This is NOT
        aggressive polling (at most 3 REST calls total, several
        seconds apart) and it NEVER guesses a final state: if every
        attempt fails, alarm_state is left exactly as it was and the
        failure is only recorded in diagnostics (last_arm).
        """

        try:
            await asyncio.sleep(ARM_CONFIRM_TIMEOUT)
        except asyncio.CancelledError:
            return

        if self.alarm_state not in TRANSIENT_ALARM_STATES:
            # A WebSocket message already resolved the transient
            # state (or cancelled this watchdog); nothing to do.
            return

        _LOGGER.warning(
            "SkylinkNet: no WebSocket confirmation received "
            "within %s seconds while alarm_state=%s, "
            "falling back to REST status check",
            ARM_CONFIRM_TIMEOUT,
            self.alarm_state,
        )

        delays = (0, *ARM_CONFIRM_RETRY_DELAYS)
        attempts = len(delays)
        last_err: str | None = None

        for attempt, delay in enumerate(delays, start=1):

            if delay:
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return

                if self.alarm_state not in TRANSIENT_ALARM_STATES:
                    return

            try:
                read = await self.api.read_devices()

            except asyncio.CancelledError:
                return

            except Exception as err:
                last_err = sanitize_exception(
                    err,
                    self.api.secrets,
                )

                _LOGGER.warning(
                    "SkylinkNet arm-confirmation REST check failed "
                    "(attempt %s/%s): %s",
                    attempt,
                    attempts,
                    last_err,
                )

                continue

            self.update_alarm_state_from_read(
                read,
                confirmed_via="rest_watchdog",
            )

            return

        _LOGGER.error(
            "SkylinkNet could not confirm the alarm state after %s "
            "REST attempts (still %s); last error: %s. Giving up "
            "without guessing the final state — a WebSocket event or "
            "the next reconnect resync will correct it.",
            attempts,
            self.alarm_state,
            last_err,
        )

        self._note_arm_confirmation_failed(last_err)

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
                        self.websocket_last_error_kind = None

                        _LOGGER.info(
                            "SkylinkNet WebSocket CONNECTED"
                        )

                        # REST resync before consuming any live event,
                        # so a snapshot can never overwrite a WebSocket
                        # event that arrived after it (see
                        # _resync_after_connect()).
                        await self._resync_after_connect()

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
                                if self._process_message(
                                    message.data
                                ):
                                    self.websocket_last_useful_message = (
                                        dt_util.utcnow()
                                    )
                                else:
                                    self.websocket_invalid_message_count += 1

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

                                    if self._process_message(
                                        text
                                    ):
                                        self.websocket_last_useful_message = (
                                            dt_util.utcnow()
                                        )
                                    else:
                                        self.websocket_invalid_message_count += 1

                                except Exception:
                                    self.websocket_invalid_message_count += 1

                                    _LOGGER.debug(
                                        "SkylinkNet binary message"
                                    )

                            elif (
                                message.type
                                == aiohttp.WSMsgType.PING
                            ):
                                # With autoping=True this branch is not
                                # normally reached (aiohttp answers the
                                # PING internally before the message
                                # reaches this loop); it is only counted
                                # here for completeness/diagnostics.
                                self.websocket_heartbeat_count += 1
                                self.websocket_last_heartbeat = (
                                    dt_util.utcnow()
                                )

                                _LOGGER.debug(
                                    "SkylinkNet WebSocket PING"
                                )

                            elif (
                                message.type
                                == aiohttp.WSMsgType.PONG
                            ):
                                self.websocket_heartbeat_count += 1
                                self.websocket_last_heartbeat = (
                                    dt_util.utcnow()
                                )

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
                                    sanitize_exception(
                                        error,
                                        self.api.secrets,
                                    )
                                    if error is not None
                                    else "WebSocket error"
                                )
                                self.websocket_last_error_kind = (
                                    type(error).__name__
                                    if error is not None
                                    else "unknown"
                                )

                                _LOGGER.error(
                                    "SkylinkNet WebSocket error: %s",
                                    self.websocket_last_error,
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

                self.websocket_last_error = sanitize_exception(
                    err,
                    self.api.secrets,
                )
                self.websocket_last_error_kind = type(err).__name__

                # _LOGGER.exception() would format err's own traceback,
                # which (for aiohttp connection errors) can embed the
                # WebSocket URL with hub_key. Log the sanitized summary
                # instead; the sanitized value is what diagnostics and
                # websocket_last_error already expose.
                _LOGGER.error(
                    "SkylinkNet WebSocket connection failed: %s",
                    self.websocket_last_error,
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
    # WEBSOCKET -> REST RESYNC
    #
    # WS CONNECT -> REST snapshot -> apply -> continue live events.
    #
    # This runs BEFORE `async for message in ws:` starts consuming
    # events (see _websocket_loop above), so a snapshot can never
    # overwrite a WebSocket event that arrived later: there isn't one
    # yet at that point. Single-flight via self._resync_lock, and
    # merge-only: any device missing from this particular snapshot is
    # left exactly as it was, never removed.
    # ============================================================

    async def _resync_after_connect(self) -> None:
        """Fetch a fresh REST snapshot right after (re)connecting."""

        if self._resync_lock.locked():
            # Another resync is already in flight (should not happen
            # given the call site, but this keeps it single-flight
            # even if that ever changes).
            return

        async with self._resync_lock:

            try:
                read = await self.api.read_devices()

            except asyncio.CancelledError:
                raise

            except Exception as err:
                _LOGGER.warning(
                    "SkylinkNet post-connect resync failed: %s",
                    sanitize_exception(
                        err,
                        self.api.secrets,
                    ),
                )
                return

            self._apply_resync_snapshot(read)

    def _apply_resync_snapshot(
        self,
        read: Any,
    ) -> None:
        """Merge a REST snapshot into current devices/states.

        Devices absent from ``read`` are left untouched (NOT removed):
        a snapshot only ever adds/updates what it actually contains.
        """

        if not isinstance(read, dict):
            return

        data = read.get("data")

        if not isinstance(data, list):
            return

        persist = False

        for item in data:

            if not isinstance(item, dict):
                continue

            dev_id = item.get("dev_id")

            if not dev_id or dev_id == ALARM_DEVICE_ID:
                continue

            if dev_id in self.ignored_device_ids:
                continue

            old_state = self.states.get(dev_id)
            new_state = dict(item)

            if old_state == new_state:
                continue

            is_new = dev_id not in self.known_device_ids

            self.states[dev_id] = new_state

            if dev_id not in self.devices:
                self.devices[dev_id] = {"dev_id": dev_id}

            if is_new:
                self.known_device_ids.add(dev_id)
                persist = True

                _LOGGER.info(
                    "SkylinkNet discovered new device via resync: %s",
                    dev_id,
                )

                self._notify_new_device_listeners(dev_id)
            else:
                persist = True

            self._notify_listeners(dev_id)

        if persist:
            self.hass.async_create_task(
                self.async_save_persisted()
            )

        # Same restore logic used at startup; it only ever sets
        # alarm_state from the virtual alarm device (F0000000) and
        # never invents a state that read() did not contain.
        self.update_alarm_state_from_read(read)

    # ============================================================
    # PROCESS MESSAGE
    # ============================================================

    def _process_message(
        self,
        message: str,
    ) -> bool:
        """Process WebSocket message.

        Returns True when the message was valid JSON that could be
        interpreted (used for the "last_useful_message"/
        "invalid_message_count" diagnostics); False for anything that
        was not a usable JSON object.
        """

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
            return False

        if not isinstance(data, dict):
            return False

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

            return True

        # Single device.
        if isinstance(payload, dict):

            self._ingest_device_item(
                payload
            )

        return True

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

            self._cancel_arm_confirmation()

            self._arming_mode = "disarmed"

            self._exit_delay = False
            self._entry_delay = False

            self.alarm_state = "disarmed"

            self._note_arm_confirmed("websocket")

            self._notify_monitor_listeners()

            return

        # ========================================================
        # ARM HOME
        # ========================================================

        if status == ALARM_CODE_ARMED_HOME:

            self._cancel_arm_confirmation()

            self._arming_mode = "armed_home"

            self._exit_delay = False
            self._entry_delay = False

            self.alarm_state = "armed_home"

            self._note_arm_confirmed("websocket")

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

            self._schedule_arm_confirmation()

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

                    self._cancel_arm_confirmation()

                    self._exit_delay = False
                    self._entry_delay = False

                    self.alarm_state = (
                        "armed_away"
                    )

                    self._note_arm_confirmed("websocket")

                else:

                    self._entry_delay = True

                    self.alarm_state = (
                        "pending"
                    )

                    self._schedule_arm_confirmation()

            else:

                self._cancel_arm_confirmation()

                self._arming_mode = (
                    "armed_away"
                )

                self._exit_delay = False
                self._entry_delay = False

                self.alarm_state = (
                    "armed_away"
                )

                self._note_arm_confirmed("websocket")

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

            self._cancel_arm_confirmation()

            self._exit_delay = False
            self._entry_delay = False

            self.alarm_state = "triggered"

            self._notify_monitor_listeners()

            return

        # ========================================================
        # TRIGGERED
        # ========================================================

        if status == ALARM_STATUS_TRIGGERED:

            self._cancel_arm_confirmation()

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
        confirmed_via: str = "rest",
    ) -> None:
        """Restore alarm state from a read() response.

        The SkylinkNet REST get_hub_status response does not contain
        the alarm state.

        The read response does contain the virtual alarm device:

            F0000000 status=4 -> disarmed
            F0000000 status=3 -> armed away

        Called during integration startup (before entities are
        created), after every WebSocket (re)connect (resync, see
        _resync_after_connect()), and by the arm-confirmation
        watchdog (confirmed_via="rest_watchdog") — always with the
        SAME restore logic, so the three sources cannot disagree.
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
                "SkylinkNet alarm state restored from REST (%s): "
                "status=%s state=%s",
                confirmed_via,
                status,
                self.alarm_state,
            )

            self._note_arm_confirmed(confirmed_via)

            self._notify_monitor_listeners()

            return

        _LOGGER.warning(
            "SkylinkNet read response does not contain "
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
        """Return WebSocket diagnostics.

        Existing keys are kept exactly as before (same names/values)
        for compatibility with anything already reading them; new
        keys are additive.
        """

        return {
            "connected": self.websocket_connected,
            "connect_count": (
                self.websocket_connect_count
            ),
            "disconnect_count": (
                self.websocket_disconnect_count
            ),
            "reconnect_count": (
                self.websocket_reconnect_count
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
            "last_error_kind": (
                self.websocket_last_error_kind
            ),
            "reconnect_delay": (
                self.websocket_reconnect_delay
            ),
            # New, finer-grained fields (see __init__ for what each
            # one means, in particular why heartbeat_count is
            # normally 0 with aiohttp autoping=True).
            "last_activity": _iso(self.websocket_last_message),
            "last_heartbeat": _iso(self.websocket_last_heartbeat),
            "last_useful_message": _iso(
                self.websocket_last_useful_message
            ),
            "heartbeat_count": self.websocket_heartbeat_count,
            "invalid_message_count": (
                self.websocket_invalid_message_count
            ),
            "last_connect": _iso(self.websocket_last_connect),
            "last_disconnect": _iso(self.websocket_last_disconnect),
        }

    def get_last_arm_info(self) -> dict[str, Any] | None:
        """Return diagnostics for the most recent arm/disarm command."""

        if self.last_arm is None:
            return None

        return {
            key: (
                _iso(value)
                if key in ("requested_at", "confirmed_at")
                else value
            )
            for key, value in self.last_arm.items()
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

        self._note_arm_requested(alarm)

        try:
            result = await self.api.set_alarm(
                alarm,
                bypass=bypass,
            )

        except Exception as err:
            message = sanitize_exception(
                err,
                self.api.secrets,
            )

            _LOGGER.error(
                "SkylinkNet alarm command failed: %s",
                message,
            )

            self._note_arm_result(f"rest_failed: {message}")

            return False

        if not isinstance(
            result,
            dict,
        ):
            _LOGGER.error(
                "SkylinkNet alarm returned invalid response: %s",
                result,
            )

            self._note_arm_result("invalid_response")

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

            self._note_arm_result(f"rejected: errno={errno}")

            return False

        self._note_arm_result("accepted")

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

            if self.alarm_state == "arming":
                self._schedule_arm_confirmation()
            else:
                self._cancel_arm_confirmation()

            self._notify_monitor_listeners()

        _LOGGER.info(
            "SkylinkNet alarm command successful: "
            "alarm=%s bypass=%s",
            alarm,
            bypass,
        )

        return True