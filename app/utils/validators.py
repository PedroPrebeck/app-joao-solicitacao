"""Utility validation helpers used across the Streamlit application.

The original implementation mixed UI and validation logic inside
``original.py`` making the helpers hard to import in isolation and almost
impossible to unit test without loading Streamlit.  The functions provided
here keep the logic pure and independent from Streamlit so that they can be
reused both in the UI code and in unit tests.
"""
from __future__ import annotations

import re
import unicodedata

__all__ = [
    "strip_accents",
    "strip_accents_and_punct_name",
    "strip_accents_and_punct_action",
    "is_valid_name",
    "is_valid_email",
]


_NON_ALPHA_REGEX = re.compile(r"[^A-Za-z\s]")
_NON_ALNUM_REGEX = re.compile(r"[^A-Za-z0-9\s]")
_EMAIL_REGEX = re.compile(r"^[a-z0-9.\_%\+\-]+@neoenergia\.com$")


def strip_accents(value: str | None) -> str:
    """Return *value* without diacritical marks.

    The helper gracefully handles ``None`` values and non-string inputs by
    returning an empty string, which mirrors the behaviour of the
    pre-refactor implementation and keeps downstream code simple.
    """

    if not isinstance(value, str):
        return ""

    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _strip_and_normalise_whitespace(value: str) -> str:
    """Condense consecutive whitespace into a single space and trim."""

    value = re.sub(r"\s+", " ", value)
    return value.strip()


def strip_accents_and_punct_name(value: str | None) -> str:
    """Return a normalised name suitable for validation and comparisons."""

    no_accents = strip_accents(value or "")
    cleaned = _NON_ALPHA_REGEX.sub(" ", no_accents)
    return _strip_and_normalise_whitespace(cleaned).upper()


def strip_accents_and_punct_action(value: str | None) -> str:
    """Return a normalised action/service description."""

    no_accents = strip_accents(value or "")
    cleaned = _NON_ALNUM_REGEX.sub(" ", no_accents)
    return _strip_and_normalise_whitespace(cleaned).upper()


def _count_words(value: str) -> int:
    return len([word for word in value.split(" ") if word])


def is_valid_name(name: str | None) -> bool:
    """Validate whether *name* contains at least two words.

    The Streamlit interface requires the requester to provide both a first
    name and a surname.  We enforce the rule by checking the cleaned version
    of the input.
    """

    cleaned = strip_accents_and_punct_name(name or "")
    return _count_words(cleaned) >= 2


def is_valid_email(email: str | None) -> bool:
    """Validate ``@neoenergia.com`` corporate e-mail addresses."""

    if not isinstance(email, str):
        return False

    return bool(_EMAIL_REGEX.match(email.strip().lower()))
