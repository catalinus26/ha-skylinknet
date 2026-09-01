"""SkylinkNet API client."""

from __future__ import annotations

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


class SkylinkNetError(Exception):
    """Base error for the SkylinkNet API."""


class SkylinkNetAuthError(SkylinkNetError):
    """Raised when login fails (wrong email/password)."""


class SkylinkNetConfigError(SkylinkNetError):
    """Raised when hub_id/hub_key are missing."""


class SkylinkNetApiError(SkylinkNetError):
    """Raised when the SkylinkNet API returns an error (errno != 0)."""


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

    # ============================================================
    # LOGIN
    # ============================================================

    async def login(self) -> dict:
        """Log in to SkylinkNet."""

        data = {
            "email": self.email,
            "password": self.password,
        }

        async with self.session.post(
            f"{API_URL}{LOGIN_ENDPOINT}",
            data=data,
            timeout=self._TIMEOUT,
        ) as response:
            response.raise_for_status()

            result = await response.json()

        if result.get("errno") != 0:
            raise SkylinkNetAuthError(
                f"SkylinkNet login failed: {result}"
            )

        self.token = result["data"]["token"]

        return result

    # ============================================================
    # HUB
    # ============================================================

    async def get_hub(self) -> dict:
        """Get hubs."""

        async with self.session.get(
            f"{API_URL}{GET_HUB_ENDPOINT}",
            timeout=self._TIMEOUT,
        ) as response:
            response.raise_for_status()

            result = await response.json()

        if result.get("errno") != 0:
            raise SkylinkNetApiError(
                f"get_hub failed: {result}"
            )

        return result

    # ============================================================
    # STATUS
    # ============================================================

    async def get_status(self) -> dict:
        """Get hub status."""

        if not self.hub_id or not self.hub_key:
            raise SkylinkNetConfigError(
                "Hub information is missing"
            )

        params = {
            "hub_id": self.hub_id,
            "key": self.hub_key,
            "op": "getstatus",
        }

        async with self.session.get(
            f"{API_URL}{GET_STATUS_ENDPOINT}",
            params=params,
            timeout=self._TIMEOUT,
        ) as response:
            response.raise_for_status()

            return await response.json()

    # ============================================================
    # DEVICES
    # ============================================================

    async def get_devices(self) -> dict:
        """Get devices."""

        if not self.hub_id or not self.hub_key:
            raise SkylinkNetConfigError(
                "Hub information is missing"
            )

        params = {
            "hub_id": self.hub_id,
            "key": self.hub_key,
        }

        async with self.session.get(
            f"{API_URL}{GET_DEV_ENDPOINT}",
            params=params,
            timeout=self._TIMEOUT,
        ) as response:
            response.raise_for_status()

            return await response.json()

    # ============================================================
    # READ DEVICE STATES
    # ============================================================

    async def read_devices(self) -> dict:
        """Read current device states."""

        if not self.hub_id or not self.hub_key:
            raise SkylinkNetConfigError(
                "Hub information is missing"
            )

        params = {
            "hub_id": self.hub_id,
            "key": self.hub_key,
        }

        async with self.session.get(
            f"{API_URL}{READ_ENDPOINT}",
            params=params,
            timeout=self._TIMEOUT,
        ) as response:
            response.raise_for_status()

            return await response.json()

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
        """

        if not self.hub_id or not self.hub_key:
            raise SkylinkNetConfigError(
                "Hub information is missing"
            )

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

        async with self.session.post(
            f"{API_URL}{SET_ALARM_ENDPOINT}",
            data=data,
            timeout=self._TIMEOUT,
        ) as response:
            response.raise_for_status()

            return await response.json()
