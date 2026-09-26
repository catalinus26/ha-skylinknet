"""SkylinkNet API client."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import aiohttp

from .const import (
    API_URL,
    GET_DEV_ENDPOINT,
    GET_HUB_ENDPOINT,
    GET_STATUS_ENDPOINT,
    LOGIN_ENDPOINT,
    READ_ENDPOINT,
    REQUEST_TIMEOUT,
    SET_ALARM_ENDPOINT,
)
from .sanitize import sanitize_exception, sanitize_text


# ============================================================
# EXCEPTIONS
#
# Every message carried by these exceptions is ALREADY sanitized
# (see sanitize.py) and none of them keeps the original exception
# as __cause__/__context__, because aiohttp exceptions embed the
# full request URL (with hub_key) in str(err).
#
# The four original classes keep their meaning:
#
#   SkylinkNetAuthError    login failed (errno != 0 on /guest/login)
#   SkylinkNetConfigError  hub_id/hub_key missing
#   SkylinkNetApiError     the API answered errno != 0
#
# New, more specific classes:
#
#   SkylinkNetConnectionError       timeout / DNS / TLS / reset
#   SkylinkNetServerError           HTTP status is not 2xx
#   SkylinkNetInvalidResponseError  body is not JSON / not an object /
#                                   lacks a required field
# ============================================================


class SkylinkNetError(Exception):
    """Base error for the SkylinkNet API."""

    #: Short machine readable category, used by diagnostics.
    kind = "error"

    def __init__(
        self,
        message: str = "",
        *,
        endpoint: str | None = None,
        status: int | None = None,
        errno: Any = None,
        kind: str | None = None,
    ) -> None:
        super().__init__(message)

        self.endpoint = endpoint
        self.http_status = status
        self.errno = errno

        if kind is not None:
            self.kind = kind


class SkylinkNetAuthError(SkylinkNetError):
    """Raised when login fails (wrong email/password)."""

    kind = "auth"


class SkylinkNetConfigError(SkylinkNetError):
    """Raised when hub_id/hub_key are missing."""

    kind = "config"


class SkylinkNetApiError(SkylinkNetError):
    """Raised when the SkylinkNet API returns an error (errno != 0)."""

    kind = "api"


class SkylinkNetConnectionError(SkylinkNetError):
    """Raised on timeout or transport level failure."""

    kind = "connection"


class SkylinkNetServerError(SkylinkNetError):
    """Raised when the server answers with a non-2xx HTTP status."""

    kind = "server"


class SkylinkNetInvalidResponseError(SkylinkNetError):
    """Raised when the response body is not the expected JSON."""

    kind = "invalid_response"


def _errno_of(result: dict) -> int | None:
    """Return ``errno`` as int, ``None`` if absent, ``-1`` if not numeric.

    This mirrors the tolerance already used for set_alarm in v0.0.2:
    a response WITHOUT ``errno`` is not treated as an error.
    """

    if "errno" not in result:
        return None

    try:
        return int(result["errno"])
    except (TypeError, ValueError):
        return -1


def _login_validator(result: dict) -> str | None:
    """A successful login must carry data.token."""

    data = result.get("data")

    if not isinstance(data, dict) or data.get("token") is None:
        return "login response has no data.token"

    return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


class SkylinkNetApi:
    """Simple SkylinkNet API client."""

    _TIMEOUT = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    def __init__(
        self,
        session: aiohttp.ClientSession,
        email: str,
        password: str,
    ) -> None:
        """Initialize API client."""

        self.session = session
        self.email = email
        self.password = password

        self.token: str | None = None
        self.hub_id: str | None = None
        self.hub_key: str | None = None

        # Diagnostics (see get_diagnostics_info()).
        self.last_operation: dict[str, Any] | None = None
        self.last_error: dict[str, Any] | None = None

    # ============================================================
    # SECRETS / SANITIZATION
    # ============================================================

    @property
    def secrets(self) -> tuple[str, ...]:
        """Credential values that must never reach logs or errors."""

        return tuple(
            value
            for value in (self.hub_key, self.password, self.token)
            if value
        )

    def _describe(self, err: BaseException) -> str:
        """Sanitized ``TypeName: message`` for an arbitrary exception."""

        return sanitize_exception(err, self.secrets)

    def _make_error(
        self,
        cls: type[SkylinkNetError],
        message: str,
        **kwargs: Any,
    ) -> SkylinkNetError:
        """Build an exception whose message is guaranteed sanitized."""

        return cls(sanitize_text(message, self.secrets), **kwargs)

    def _require_hub(self) -> None:
        """Raise SkylinkNetConfigError when hub credentials are missing."""

        if not self.hub_id or not self.hub_key:
            raise SkylinkNetConfigError(
                "Hub information is missing",
                kind="config",
            )

    # ============================================================
    # CENTRAL REQUEST
    # ============================================================

    async def _request(
        self,
        operation: str,
        method: str,
        endpoint: str,
        *,
        params: dict | None = None,
        data: dict | None = None,
        errno_error: type[SkylinkNetError] | None = None,
        validator: Callable[[dict], str | None] | None = None,
    ) -> dict:
        """Perform a request and return the decoded JSON object.

        Centralizes timeout, HTTP status, JSON decoding, response shape,
        errno handling, sanitization and diagnostics.

        ``errno_error``: when given, ``errno != 0`` (a MISSING errno also
        counts, exactly like v0.0.2 did for login/get_hub) raises that
        class. When ``None`` the errno is not enforced (v0.0.2 behaviour
        for get_status/get_devices/read_devices/set_alarm).

        Exceptions are raised OUTSIDE any ``except`` block so they carry
        neither ``__cause__`` nor ``__context__``: the original aiohttp
        exception (which holds the URL with hub_key) is not reachable.
        """

        url = f"{API_URL}{endpoint}"
        started = time.monotonic()

        error: SkylinkNetError | None = None
        status: int | None = None
        body: bytes = b""
        result: Any = None

        try:
            async with self.session.request(
                method,
                url,
                params=params,
                data=data,
                timeout=self._TIMEOUT,
            ) as response:
                status = response.status

                if not 200 <= status < 300:
                    error = self._make_error(
                        SkylinkNetServerError,
                        f"{method} {endpoint} returned HTTP {status}",
                        endpoint=endpoint,
                        status=status,
                    )
                else:
                    body = await response.read()

        except asyncio.TimeoutError:
            error = self._make_error(
                SkylinkNetConnectionError,
                f"Timeout after {REQUEST_TIMEOUT}s calling "
                f"{method} {endpoint}",
                endpoint=endpoint,
                kind="timeout",
            )

        except (aiohttp.ClientError, OSError) as err:
            error = self._make_error(
                SkylinkNetConnectionError,
                f"{method} {endpoint} failed: {self._describe(err)}",
                endpoint=endpoint,
                status=status,
            )

        except Exception as err:  # noqa: BLE001 - must never leak a secret
            error = self._make_error(
                SkylinkNetError,
                f"Unexpected error on {method} {endpoint}: "
                f"{self._describe(err)}",
                endpoint=endpoint,
                status=status,
                kind="unexpected",
            )

        errno: Any = None

        if error is None:
            try:
                result = json.loads(body)
            except (json.JSONDecodeError, UnicodeDecodeError):
                # json.loads(body) fails in two ways: malformed JSON
                # (json.JSONDecodeError) or a body that isn't valid
                # UTF-8 (UnicodeDecodeError). Both are listed
                # explicitly here for clarity, even though
                # UnicodeDecodeError -> UnicodeError -> ValueError in
                # Python's stdlib, so `except ValueError` alone would
                # already have caught both. The body itself is
                # deliberately NOT echoed.
                error = self._make_error(
                    SkylinkNetInvalidResponseError,
                    f"{method} {endpoint} returned invalid JSON",
                    endpoint=endpoint,
                    status=status,
                )

        if error is None and not isinstance(result, dict):
            error = self._make_error(
                SkylinkNetInvalidResponseError,
                f"{method} {endpoint} returned "
                f"{type(result).__name__}, expected a JSON object",
                endpoint=endpoint,
                status=status,
            )

        if error is None:
            errno = _errno_of(result)

            if errno_error is not None and result.get("errno") != 0:
                error = self._make_error(
                    errno_error,
                    f"{operation} failed: errno={result.get('errno')!r}"
                    f" ({_summary(result)})",
                    endpoint=endpoint,
                    status=status,
                    errno=result.get("errno"),
                )

            elif validator is not None:
                problem = validator(result)

                if problem:
                    error = self._make_error(
                        SkylinkNetInvalidResponseError,
                        f"{operation}: {problem}",
                        endpoint=endpoint,
                        status=status,
                    )

        self._record(
            operation,
            method,
            endpoint,
            started,
            status,
            errno,
            error,
        )

        if error is not None:
            raise error

        return result

    def _record(
        self,
        operation: str,
        method: str,
        endpoint: str,
        started: float,
        status: int | None,
        errno: Any,
        error: SkylinkNetError | None,
    ) -> None:
        """Remember the last operation / last error for diagnostics."""

        now = datetime.now(timezone.utc)

        self.last_operation = {
            "operation": operation,
            "method": method,
            "endpoint": endpoint,
            "at": now,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "ok": error is None,
            "http_status": status,
            "errno": errno,
        }

        if error is not None:
            self.last_error = {
                "operation": operation,
                "endpoint": endpoint,
                "at": now,
                "type": type(error).__name__,
                "kind": error.kind,
                "http_status": error.http_status,
                "errno": error.errno,
                "message": str(error),
            }

    def get_diagnostics_info(self) -> dict[str, Any]:
        """Return sanitized API diagnostics (no credentials)."""

        def render(item: dict[str, Any] | None) -> dict[str, Any] | None:
            if item is None:
                return None

            return {
                key: (_iso(value) if isinstance(value, datetime) else value)
                for key, value in item.items()
            }

        last_error = render(self.last_error)

        return {
            "last_operation": render(self.last_operation),
            "last_error": last_error,
            "last_error_type": (
                last_error["type"] if last_error else None
            ),
        }

    # ============================================================
    # LOGIN
    # ============================================================

    async def login(self) -> dict:
        """Log in to SkylinkNet."""

        data = {
            "email": self.email,
            "password": self.password,
        }

        result = await self._request(
            "login",
            "POST",
            LOGIN_ENDPOINT,
            data=data,
            errno_error=SkylinkNetAuthError,
            validator=_login_validator,
        )

        self.token = result["data"]["token"]

        return result

    # ============================================================
    # HUB
    # ============================================================

    async def get_hub(self) -> dict:
        """Get hubs."""

        return await self._request(
            "get_hub",
            "GET",
            GET_HUB_ENDPOINT,
            errno_error=SkylinkNetApiError,
        )

    # ============================================================
    # STATUS
    # ============================================================

    async def get_status(self) -> dict:
        """Get hub status."""

        self._require_hub()

        params = {
            "hub_id": self.hub_id,
            "key": self.hub_key,
            "op": "getstatus",
        }

        return await self._request(
            "get_status",
            "GET",
            GET_STATUS_ENDPOINT,
            params=params,
        )

    async def validate_hub(self) -> dict:
        """Check that hub_id/hub_key are accepted (used by the config flow).

        Only an EXPLICIT non-zero ``errno`` is treated as a rejection; a
        response without ``errno`` is accepted, like everywhere else.

        UNKNOWN: which ``errno`` the server returns for a wrong hub_key
        has not been observed yet. It is reported as SkylinkNetApiError
        and surfaces in the config flow as ``cannot_connect`` (NOT
        ``invalid_auth``, which is reserved for the account login).
        """

        result = await self.get_status()

        errno = _errno_of(result)

        if errno not in (None, 0):
            error = self._make_error(
                SkylinkNetApiError,
                f"get_status rejected the hub credentials: "
                f"errno={result.get('errno')!r} ({_summary(result)})",
                endpoint=GET_STATUS_ENDPOINT,
                errno=result.get("errno"),
            )

            self.last_error = {
                "operation": "validate_hub",
                "endpoint": GET_STATUS_ENDPOINT,
                "at": datetime.now(timezone.utc),
                "type": type(error).__name__,
                "kind": error.kind,
                "http_status": None,
                "errno": error.errno,
                "message": str(error),
            }

            raise error

        return result

    # ============================================================
    # DEVICES
    # ============================================================

    async def get_devices(self) -> dict:
        """Get devices."""

        self._require_hub()

        params = {
            "hub_id": self.hub_id,
            "key": self.hub_key,
        }

        return await self._request(
            "get_devices",
            "GET",
            GET_DEV_ENDPOINT,
            params=params,
        )

    # ============================================================
    # READ DEVICE STATES
    # ============================================================

    async def read_devices(self) -> dict:
        """Read current device states."""

        self._require_hub()

        params = {
            "hub_id": self.hub_id,
            "key": self.hub_key,
        }

        return await self._request(
            "read_devices",
            "GET",
            READ_ENDPOINT,
            params=params,
        )

    # ============================================================
    # ALARM
    # ============================================================

    async def set_alarm(
        self,
        alarm: str,
        bypass: str | None = None,
    ) -> dict:
        """Set SkylinkNet alarm state.

        Supported commands:

        - disarm
        - arm_home
        - arm_away

        Optional bypass:
        - "1"

        NOTE: what ``bypass="1"`` means at protocol level is UNKNOWN.
        It is forwarded exactly as before and nothing is inferred from it.
        """

        self._require_hub()

        if alarm not in (
            "disarm",
            "arm_home",
            "arm_away",
        ):
            raise ValueError(
                f"Unsupported alarm command: {alarm}"
            )

        data = {
            "hub_id": self.hub_id,
            "key": self.hub_key,
            "alarm": alarm,
        }

        if bypass is not None:
            data["bypass"] = bypass

        return await self._request(
            "set_alarm",
            "POST",
            SET_ALARM_ENDPOINT,
            data=data,
        )


def _summary(result: dict, limit: int = 200) -> str:
    """Short, single-line rendering of a response for error messages."""

    text = repr(result)

    return text if len(text) <= limit else text[: limit - 3] + "..."
