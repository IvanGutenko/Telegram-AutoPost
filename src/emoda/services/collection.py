from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from emoda.domain.schemas import CollectionBoundaryRule, CollectionProfile


@dataclass(frozen=True)
class CollectionMessage:
    message_id: int
    occurred_at: datetime
    grouped_id: int | str | None = None
    text: str = ""
    caption: str = ""
    media_count: int = 0

    @property
    def visible_text(self) -> str:
        return self.text.strip() or self.caption.strip()

    @property
    def has_text(self) -> bool:
        return bool(self.visible_text)

    @property
    def has_media(self) -> bool:
        return self.media_count > 0


class CollectionAction(StrEnum):
    APPEND = "append"
    START_NEW = "start_new"
    APPEND_AND_CLOSE = "append_and_close"
    IGNORE = "ignore"


@dataclass(frozen=True)
class CollectionDecision:
    action: CollectionAction
    reason: str
    rule_name: str | None = None


@dataclass
class CollectionBatch:
    messages: list[CollectionMessage] = field(default_factory=list)
    closed: bool = False


@dataclass(frozen=True)
class CollectionPartition:
    products: list[CollectionBatch]
    ignored: list[CollectionMessage]


class CollectionEngine:
    """Pure product-boundary engine selected only by validated profile data."""

    def __init__(self, profile: CollectionProfile) -> None:
        self.profile = profile

    def partition(self, messages: list[CollectionMessage]) -> CollectionPartition:
        products: list[CollectionBatch] = []
        ignored: list[CollectionMessage] = []
        for incoming in sorted(messages, key=lambda item: (item.occurred_at, item.message_id)):
            current = products[-1] if products else None
            decision = self.decide(current.messages if current else [], incoming)
            if decision.action is CollectionAction.IGNORE:
                ignored.append(incoming)
                continue
            if current is None or current.closed or decision.action is CollectionAction.START_NEW:
                current = CollectionBatch()
                products.append(current)
            current.messages.append(incoming)
            if decision.action is CollectionAction.APPEND_AND_CLOSE:
                current.closed = True
        return CollectionPartition(products=products, ignored=ignored)

    def decide(
        self, current: list[CollectionMessage], incoming: CollectionMessage
    ) -> CollectionDecision:
        ignored = self._first_match("ignore", current, incoming)
        if ignored:
            return CollectionDecision(CollectionAction.IGNORE, ignored.reason or ignored.name, ignored.name)
        if not current:
            end = self._first_match("end", current, incoming)
            return CollectionDecision(
                CollectionAction.APPEND_AND_CLOSE if end else CollectionAction.APPEND,
                "first message",
                end.name if end else None,
            )

        # Telegram albums are atomic transport units. A caption may live on any
        # member of the same grouped_id; neither that caption nor a start rule
        # may split one album into several products.
        is_transport_group = incoming.grouped_id is not None and not (
            isinstance(incoming.grouped_id, str)
            and incoming.grouped_id.startswith("__")
        )
        if is_transport_group and any(
            item.grouped_id == incoming.grouped_id for item in current
        ):
            return CollectionDecision(
                CollectionAction.APPEND,
                "same grouped_id continuation",
            )

        hard = self._first_match("hard_boundary", current, incoming)
        if hard:
            closes_media_first = (
                self.profile.order == "media_before_text"
                and incoming.has_text
                and any(item.has_media for item in current)
                and not any(item.has_text for item in current)
            )
            return CollectionDecision(
                CollectionAction.APPEND_AND_CLOSE
                if closes_media_first
                else CollectionAction.START_NEW,
                "hard boundary",
                hard.name,
            )

        limited = self._limit_reason(current, incoming)
        if limited:
            return CollectionDecision(CollectionAction.START_NEW, limited)

        start = self._first_match("start", current, incoming)
        if start and self.profile.stop_on_next_start:
            return CollectionDecision(CollectionAction.START_NEW, "next product start", start.name)

        strategy_reason = self._strategy_boundary(current, incoming)
        if strategy_reason:
            return CollectionDecision(CollectionAction.START_NEW, strategy_reason)

        continuation = self._first_match("continue", current, incoming)
        soft = self._first_match("soft_boundary", current, incoming)
        if soft and continuation is None:
            return CollectionDecision(CollectionAction.START_NEW, "soft boundary", soft.name)

        end = self._first_match("end", current, incoming)
        if end:
            return CollectionDecision(CollectionAction.APPEND_AND_CLOSE, "end boundary", end.name)
        return CollectionDecision(
            CollectionAction.APPEND,
            "explicit continuation" if continuation else "profile permits merge",
            continuation.name if continuation else None,
        )

    def _limit_reason(
        self, current: list[CollectionMessage], incoming: CollectionMessage
    ) -> str | None:
        latest = current[-1]
        if (
            self.profile.max_time_gap_seconds > 0
            and (incoming.occurred_at - latest.occurred_at).total_seconds()
            > float(self.profile.max_time_gap_seconds)
        ):
            return "time gap exceeded"
        if (
            self.profile.max_message_id_gap > 0
            and incoming.message_id - latest.message_id > self.profile.max_message_id_gap
        ):
            return "message ID gap exceeded"
        if len(current) + 1 > self.profile.max_messages:
            return "message limit exceeded"
        if sum(item.media_count for item in current) + incoming.media_count > self.profile.max_media:
            return "media limit exceeded"
        return None

    def _strategy_boundary(
        self, current: list[CollectionMessage], incoming: CollectionMessage
    ) -> str | None:
        if self.profile.strategy == "single_message":
            return "single-message strategy"
        if self.profile.strategy == "single_grouped_id":
            group_ids = {item.grouped_id for item in current if item.grouped_id is not None}
            if incoming.grouped_id is None or not group_ids or incoming.grouped_id not in group_ids:
                return "grouped-ID boundary"

        text_values = {self._normalize(item.visible_text) for item in current if item.has_text}
        incoming_text = self._normalize(incoming.visible_text)
        is_new_text = bool(incoming_text and incoming_text not in text_values)
        if is_new_text and text_values and not self.profile.allow_multiple_text_blocks:
            return "multiple text blocks disabled"

        if self.profile.order == "media_before_text":
            if any(item.has_text for item in current):
                return "media-before-text product already closed"
            if incoming.has_text:
                return None
        elif self.profile.order == "text_before_media":
            if incoming.has_text and any(item.has_media for item in current):
                return "next text-before-media product"
            if not any(item.has_text for item in current) and incoming.has_media:
                return "text-before-media requires opening text"
        elif self.profile.order == "text_between_media":
            if is_new_text and text_values and not self.profile.allow_multiple_text_blocks:
                return "next text block"
        return None

    def _first_match(
        self,
        action: str,
        current: list[CollectionMessage],
        incoming: CollectionMessage,
    ) -> CollectionBoundaryRule | None:
        return next(
            (
                rule
                for rule in self.profile.boundary_rules
                if rule.enabled and rule.action == action and self._matches(rule, current, incoming)
            ),
            None,
        )

    @staticmethod
    def _normalize(value: str) -> str:
        return " ".join(value.split()).casefold()

    def _matches(
        self,
        rule: CollectionBoundaryRule,
        current: list[CollectionMessage],
        incoming: CollectionMessage,
    ) -> bool:
        current_text = "\n".join(item.visible_text for item in current if item.has_text)
        text = (
            incoming.visible_text
            if rule.target == "incoming"
            else current_text
            if rule.target == "current"
            else "\n".join(filter(None, (current_text, incoming.visible_text)))
        )
        if rule.match == "regex":
            return re.search(rule.pattern or "", text, re.IGNORECASE if rule.ignore_case else 0) is not None
        if rule.match == "keyword":
            candidate = text.casefold() if rule.ignore_case else text
            return any(
                (keyword.casefold() if rule.ignore_case else keyword) in candidate
                for keyword in rule.keywords
            )
        if rule.match == "has_text":
            return incoming.has_text
        if rule.match == "has_media":
            return incoming.has_media
        if rule.match == "captioned_media":
            return incoming.has_media and incoming.has_text
        if rule.match == "grouped_id_change":
            prior = next((item.grouped_id for item in reversed(current) if item.grouped_id is not None), None)
            return prior is not None and incoming.grouped_id is not None and incoming.grouped_id != prior
        return rule.match == "always"

    def main_text(self, messages: list[CollectionMessage]) -> str:
        texts = self._unique_texts(messages)
        if not texts:
            return ""
        if self.profile.main_text == "first":
            return texts[0]
        if self.profile.main_text == "last":
            return texts[-1]
        if self.profile.main_text == "longest":
            return max(texts, key=len)
        if self.profile.main_text == "caption":
            captions = self._unique(item.caption for item in messages if item.caption.strip())
            return captions[0] if captions else texts[0]
        return "\n".join(texts)

    def merged_text(self, messages: list[CollectionMessage]) -> str:
        texts = self._unique_texts(messages)
        if self.profile.merge_text_blocks or self.profile.main_text == "all":
            return "\n".join(texts)
        return self.main_text(messages)

    def _unique_texts(self, messages: list[CollectionMessage]) -> list[str]:
        return self._unique(item.visible_text for item in messages if item.has_text)

    @classmethod
    def _unique(cls, values: Iterable[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            cleaned = str(value).strip()
            identity = cls._normalize(cleaned)
            if cleaned and identity not in seen:
                seen.add(identity)
                result.append(cleaned)
        return result
