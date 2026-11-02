from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

SYSTEM_CURRENCY_ALIASES: tuple[tuple[str, str], ...] = (
    ("$", "USD"),
    ("💲", "USD"),
    ("💵", "USD"),
    ("USD", "USD"),
    ("US$", "USD"),
    ("€", "EUR"),
    ("EUR", "EUR"),
)


def normalize_currency_alias_key(alias: str) -> str:
    """Stable identity used by both the database and the runtime resolver."""

    return alias.strip().casefold()


@dataclass(frozen=True)
class CurrencyAliasMatch:
    currency: str
    alias: str


class CurrencyAliasResolver:
    """Resolve only explicitly registered currency aliases.

    Production constructs this object from enabled ``currency_aliases`` rows.
    The system defaults constructor is retained for isolated pure-engine calls;
    it contains the exact same seed data as the migration, not a second matching
    algorithm.
    """

    def __init__(self, aliases: Mapping[str, str]) -> None:
        normalized: dict[str, CurrencyAliasMatch] = {}
        for raw_alias, raw_currency in aliases.items():
            alias = str(raw_alias).strip()
            currency = str(raw_currency).strip().upper()
            if not alias or not re.fullmatch(r"[A-Z]{3}", currency):
                raise ValueError("currency aliases require a non-empty alias and ISO-like code")
            normalized[normalize_currency_alias_key(alias)] = CurrencyAliasMatch(
                currency=currency,
                alias=alias,
            )
        self._aliases = normalized
        alternatives = sorted(
            (re.escape(item.alias) for item in normalized.values()),
            key=len,
            reverse=True,
        )
        self._pattern = re.compile("|".join(alternatives), re.IGNORECASE) if alternatives else None

    @classmethod
    def system_defaults(cls) -> CurrencyAliasResolver:
        return cls(dict(SYSTEM_CURRENCY_ALIASES))

    def resolve(self, value: str | None) -> CurrencyAliasMatch | None:
        if value is None:
            return None
        return self._aliases.get(normalize_currency_alias_key(value))

    def find_in_context(
        self, *, matched_text: str, before: str, after: str
    ) -> CurrencyAliasMatch | None:
        """Find one unambiguous registered currency beside a price match."""

        if self._pattern is None:
            return None
        found = list(self._pattern.finditer(matched_text))
        before_match = None
        for candidate in self._pattern.finditer(before):
            if not before[candidate.end() :].strip():
                before_match = candidate
        if before_match is not None:
            found.append(before_match)
        after_match = self._pattern.match(after.lstrip())
        if after_match is not None:
            tail = after.lstrip()[after_match.end() : after_match.end() + 1]
            if not tail or not tail.isalpha():
                found.append(after_match)
        resolved = [self.resolve(candidate.group(0)) for candidate in found]
        matches = [item for item in resolved if item is not None]
        currencies = {item.currency for item in matches}
        if len(currencies) != 1 or not matches:
            return None
        return matches[0]

    def as_dict(self) -> dict[str, str]:
        return {item.alias: item.currency for item in self._aliases.values()}
