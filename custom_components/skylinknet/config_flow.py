"""Config flow for SkylinkNet."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components.binary_sensor import BinarySensorDeviceClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import (
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from .api import SkylinkNetApi, SkylinkNetAuthError
from .const import (
    CONF_DEFAULT_DEVICE_CLASS,
    DEFAULT_DEVICE_CLASS,
    DEVICE_CLASS_NONE,
    DOMAIN,
)

# ============================================================
# DEFAULT DEVICE CLASS OPTIONS
#
# Folosită doar pentru dispozitive descoperite dinamic prin
# WebSocket, pentru care nu putem determina automat door/window/
# motion din "dev_type" (vezi binary_sensor.py).
# ============================================================

DEVICE_CLASS_OPTIONS = [
    DEVICE_CLASS_NONE,
    BinarySensorDeviceClass.MOTION.value,
    BinarySensorDeviceClass.DOOR.value,
    BinarySensorDeviceClass.WINDOW.value,
    BinarySensorDeviceClass.OPENING.value,
    BinarySensorDeviceClass.MOISTURE.value,
]


class SkylinkNetConfigFlow(
    config_entries.ConfigFlow,
    domain=DOMAIN,
):
    """Handle a config flow for SkylinkNet."""

    VERSION = 1

    _reauth_entry: ConfigEntry | None = None

    async def async_step_user(
        self,
        user_input: dict | None = None,
    ):
        """Handle the initial step."""

        errors = {}

        if user_input is not None:
            try:
                session = async_get_clientsession(self.hass)

                api = SkylinkNetApi(
                    session,
                    user_input[CONF_EMAIL],
                    user_input[CONF_PASSWORD],
                )

                await api.login()

            except (aiohttp.ClientError, KeyError, ValueError):
                errors["base"] = "cannot_connect"

            except SkylinkNetAuthError:
                errors["base"] = "invalid_auth"

            else:
                hub_id = user_input["hub_id"]

                await self.async_set_unique_id(hub_id)
                self._abort_if_unique_id_configured()

                return self.async_create_entry(
                    title=f"SkylinkNet Hub {hub_id}",
                    data=user_input,
                    options={
                        CONF_DEFAULT_DEVICE_CLASS: DEFAULT_DEVICE_CLASS,
                    },
                )

        schema = vol.Schema(
            {
                vol.Required(CONF_EMAIL): str,
                vol.Required(CONF_PASSWORD): str,
                vol.Required("hub_id"): str,
                vol.Required("hub_key"): str,
            }
        )

        return self.async_show_form(
            step_id="user",
            data_schema=schema,
            errors=errors,
        )

    @staticmethod
    def async_get_options_flow(
        config_entry: ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Return the options flow."""

        return SkylinkNetOptionsFlow(config_entry)

    # ============================================================
    # REAUTHENTICATION
    #
    # Declanșat automat de Home Assistant când async_setup_entry
    # ridică ConfigEntryAuthFailed (ex: parola a fost schimbată).
    # ============================================================

    async def async_step_reauth(
        self,
        entry_data: Mapping[str, Any],
    ):
        """Handle reauthentication triggered by HA."""

        self._reauth_entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )

        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self,
        user_input: dict | None = None,
    ):
        """Ask the user for new credentials."""

        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                session = async_get_clientsession(self.hass)

                api = SkylinkNetApi(
                    session,
                    user_input[CONF_EMAIL],
                    user_input[CONF_PASSWORD],
                )

                await api.login()

            except (aiohttp.ClientError, KeyError, ValueError):
                errors["base"] = "cannot_connect"

            except SkylinkNetAuthError:
                errors["base"] = "invalid_auth"

            else:
                new_data = {
                    **self._reauth_entry.data,
                    CONF_EMAIL: user_input[CONF_EMAIL],
                    CONF_PASSWORD: user_input[CONF_PASSWORD],
                }

                self.hass.config_entries.async_update_entry(
                    self._reauth_entry,
                    data=new_data,
                )

                await self.hass.config_entries.async_reload(
                    self._reauth_entry.entry_id
                )

                return self.async_abort(
                    reason="reauth_successful"
                )

        schema = vol.Schema(
            {
                vol.Required(CONF_EMAIL): str,
                vol.Required(CONF_PASSWORD): str,
            }
        )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=schema,
            errors=errors,
        )


class SkylinkNetOptionsFlow(config_entries.OptionsFlow):
    """Handle SkylinkNet options.

    Momentan singura opțiune este clasa implicită de senzor,
    folosită pentru dispozitive descoperite dinamic prin
    WebSocket și pentru care nu cunoaștem "dev_type"-ul.
    """

    def __init__(self, config_entry: ConfigEntry) -> None:
        """Initialize options flow."""

        self._config_entry = config_entry

    async def async_step_init(
        self,
        user_input: dict | None = None,
    ):
        """Manage options."""

        if user_input is not None:
            return self.async_create_entry(
                title="",
                data=user_input,
            )

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_DEFAULT_DEVICE_CLASS,
                        default=self._config_entry.options.get(
                            CONF_DEFAULT_DEVICE_CLASS,
                            DEFAULT_DEVICE_CLASS,
                        ),
                    ): SelectSelector(
                        SelectSelectorConfig(
                            options=DEVICE_CLASS_OPTIONS,
                            mode=SelectSelectorMode.DROPDOWN,
                        )
                    )
                }
            ),
        )
