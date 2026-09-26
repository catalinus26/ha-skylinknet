"""Secret sanitization helpers for SkylinkNet.

Design rules
------------
* Sanitize AT THE SOURCE: every place that turns an exception or a URL
  into text (API errors, WebSocket errors, logs, diagnostics) calls into
  this module. There is deliberately no global logging filter.
* Secrets are ``hub_key``, ``password`` and ``token``.
  ``hub_id`` is an identifier, NOT a secret, and is never redacted here.
* Two layers of protection are applied to every string:

  1. literal replacement of the secret values we actually know
     (plain, URL-quoted and form-quoted variants);
  2. pattern based redaction of the places a credential travels
     (``key=...`` / ``password=...`` query or form fields, dict/JSON
     reprs such as ``'hub_key': '...'`` and the ``/websock/hu/<id>/<key>``
     WebSocket path).

This module is pure Python on purpose: it has no Home Assistant import,
so it can be unit tested (and used from ``api.py``) without a HA stub.
The Home Assistant ``async_redact_data`` helper is used separately in
``diagnostics.py``.
"""

from __future__ import annotations

import re
import traceback
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import quote, quote_plus

REDACTED = "**REDACTED**"

# Credentials. NOTE: ``hub_id`` is intentionally absent.
SECRET_FIELD_NAMES = frozenset({"hub_key", "password", "token"})

# Keys redacted by Home Assistant's ``async_redact_data`` in diagnostics.
#
# * ``key`` is the name of the REST query parameter that carries hub_key.
# * ``email`` is personal data; it was already redacted in v0.0.2 and is
#   kept that way.
# * ``hub_id`` is NOT in this set (it is not a credential).
DIAGNOSTICS_TO_REDACT = frozenset(
    {"email", "password", "hub_key", "token", "key"}
)

# Literal replacement is skipped for values shorter than this, otherwise a
# 1-2 character secret would garble every message. Real credentials are far
# longer; the pattern layer still covers where they travel.
_MIN_LITERAL_LEN = 3

_NAMES = r"(?:hub_key|password|token|key)"

# key=VALUE  (query string or form encoding)
_RE_QUERY = re.compile(
    rf"(?i)(?<![A-Za-z0-9_])({_NAMES})=([^&\s'\"<>]+)"
)

# 'key': 'VALUE'  /  "password": "VALUE"   (python or JSON repr)
_RE_REPR = re.compile(
    rf"(?i)(['\"]{_NAMES}['\"]\s*:\s*)(['\"])(.*?)\2"
)

# /websock/hu/<hub_id>/<hub_key>   -> keep hub_id, hide hub_key
_RE_WS_PATH = re.compile(
    r"(/websock/hu/[^/\s'\"?#]+/)([^/\s'\"?#]+)"
)


def _literal_variants(secret: str) -> set[str]:
    """Return the spellings a secret can take inside URLs/forms."""

    return {
        secret,
        quote(secret, safe=""),
        quote_plus(secret),
    }


def sanitize_text(
    text: Any,
    secrets: Iterable[str | None] = (),
) -> str:
    """Return ``text`` with every known secret and credential pattern hidden."""

    if not isinstance(text, str):
        text = str(text)

    # Layer 1: literal, longest first so a secret that contains another
    # one is replaced as a whole.
    variants: set[str] = set()

    for secret in secrets:
        if isinstance(secret, str) and len(secret) >= _MIN_LITERAL_LEN:
            variants |= _literal_variants(secret)

    for variant in sorted(variants, key=len, reverse=True):
        if variant:
            text = text.replace(variant, REDACTED)

    # Layer 2: patterns.
    text = _RE_QUERY.sub(rf"\1={REDACTED}", text)
    text = _RE_REPR.sub(rf"\1\2{REDACTED}\2", text)
    text = _RE_WS_PATH.sub(rf"\1{REDACTED}", text)

    return text


def sanitize_url(
    url: Any,
    secrets: Iterable[str | None] = (),
) -> str:
    """Return a URL that is safe to log (query/path credentials hidden)."""

    return sanitize_text(url, secrets)


def sanitize_exception(
    err: BaseException,
    secrets: Iterable[str | None] = (),
) -> str:
    """Return ``"TypeName: message"`` for ``err`` with secrets hidden.

    aiohttp exceptions (``ClientResponseError``, ``WSServerHandshakeError``)
    embed the full request URL - including ``key=...`` or the
    ``/websock/hu/<id>/<key>`` path - in ``str(err)``. Both are handled.
    """

    name = type(err).__name__
    message = sanitize_text(str(err), secrets).strip()

    return f"{name}: {message}" if message else name


def format_sanitized_traceback(
    err: BaseException,
    secrets: Iterable[str | None] = (),
) -> str:
    """Return the full formatted traceback of ``err`` with secrets hidden.

    ``traceback.format_exception`` follows ``__cause__`` / ``__context__``,
    which is exactly where a leaked URL would otherwise survive.
    """

    text = "".join(
        traceback.format_exception(type(err), err, err.__traceback__)
    )

    return sanitize_text(text, secrets)


def secrets_from_mapping(data: Mapping[str, Any] | None) -> tuple[str, ...]:
    """Collect secret values (hub_key/password/token) from a mapping."""

    if not data:
        return ()

    return tuple(
        value
        for name, value in data.items()
        if name in SECRET_FIELD_NAMES
        and isinstance(value, str)
        and value
    )


def scrub_deep(
    obj: Any,
    secrets: Iterable[str | None] = (),
) -> Any:
    """Return a copy of ``obj`` whose string values are sanitized.

    Used as a last line of defence on diagnostics output: the key based
    ``async_redact_data`` cannot see a secret embedded in a value such as
    an error message.
    """

    secrets = tuple(secrets)

    if isinstance(obj, str):
        return sanitize_text(obj, secrets)

    if isinstance(obj, Mapping):
        return {key: scrub_deep(value, secrets) for key, value in obj.items()}

    if isinstance(obj, (list, tuple, set, frozenset)):
        return [scrub_deep(value, secrets) for value in obj]

    return obj
