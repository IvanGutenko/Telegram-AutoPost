from __future__ import annotations

import re

from emoda.domain.schemas import CleaningRule


class FinalSanitationError(ValueError):
    pass


GLOBAL_PATTERNS = (
    r"(?im)^\s*(?:order(?:ing)?\s*(?:line|hotline)|order\s+contact|"
    r"sales\s+(?:line|hotline)|customer\s+service|contact\s+us|"
    r"sipari(?:ş|s)\s+hatt[ıi]|m[üu]şteri\s+hizmetleri|"
    r"линия\s+заказов|служба\s+заказов)(?:\s*[:\-–—].*)?\s*$",
    r"(?:https?://)?t\.me/\S+",
    r"(?:https?://)?(?:wa\.me|api\.whatsapp\.com)/\S+",
    r"(?<!\w)@\w{4,}",
    r"(?im)\b(?:supplier\s+)?(?:price|цена|fiyat)\s*[:\-]?\s*(?:[$€]\s*)?"
    r"\d+(?:[.,]\d+)?\s*(?:USD|EUR|[$€])?",
    r"(?<!\w)(?:[$€]\s*\d+(?:[.,]\d+)?|\d+(?:[.,]\d+)?\s*(?:USD|EUR|[$€]))(?!\w)",
    r"(?im)^.*\b(?:whatsapp|ватсап|вацап)\b.*$",
    r"(?<!\w)(?:\+\d(?:[ \t().-]*\d){7,}|(?=[\d \t().-]{8,}(?!\w))"
    r"(?=[\d \t().-]*[ \t().-])\d(?:[ \t().-]*\d){7,}(?!\w))",
)

MODEL_META_PATTERNS = (
    r"(?i)\bplease\s+provide\b.*\b(?:text|description|content|context)\b",
    r"(?i)\bi\s+(?:cannot|can't|am unable to)\s+(?:translate|help|process)\b",
    r"(?i)```(?:json|python|text)?",
    r"(?i)\b(?:system|assistant|developer)\s+(?:message|instruction|prompt)\b",
    r"(?i)__[^\n]*(?:debug|placeholder)[^\n]*__",
)


class FinalSanitizer:
    @classmethod
    def apply_source_rules(cls, text: str, rules: list[CleaningRule]) -> str:
        """Apply enabled source-wide cleaning before extraction.

        Language-specific rules remain available for legacy translated output;
        only ``both`` rules are safe to apply to the untranslated supplier input.
        """
        cleaned = text
        for rule in rules:
            if rule.enabled and rule.language == "both":
                cleaned = cls._apply_rule(cleaned, rule)
        return cleaned

    def sanitize(
        self,
        text: str,
        *,
        language: str,
        rules: list[CleaningRule],
        validate_language: bool,
    ) -> str:
        cleaned = text
        for pattern in GLOBAL_PATTERNS:
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
        for rule in rules:
            if not rule.enabled:
                continue
            if rule.language not in {"both", language.casefold()}:
                continue
            cleaned = self._apply_rule(cleaned, rule)
        cleaned = self._normalize(cleaned)
        if any(re.search(pattern, cleaned) for pattern in MODEL_META_PATTERNS):
            raise FinalSanitationError("model meta, placeholder or debug output rejected")
        if re.search(r"__(?:P\d+|PRESERVE_\d+)__", cleaned):
            raise FinalSanitationError("unresolved placeholder or debug output rejected")
        if validate_language and cleaned:
            self._validate_language(cleaned, language)
        return cleaned

    @staticmethod
    def _apply_rule(text: str, rule: CleaningRule) -> str:
        flags = re.IGNORECASE if rule.ignore_case else 0
        if rule.kind == "regex":
            return re.sub(rule.value, rule.replacement, text, flags=flags)
        if rule.scope == "text":
            if rule.kind == "exact" and FinalSanitizer._compare(text.strip(), rule.value, flags):
                return rule.replacement
            pattern = re.escape(rule.value)
            if rule.kind == "contains":
                return re.sub(pattern, rule.replacement, text, flags=flags)
            if rule.kind == "prefix":
                return re.sub(rf"^\s*{pattern}", rule.replacement, text, flags=flags)
            if rule.kind == "suffix":
                return re.sub(rf"{pattern}\s*$", rule.replacement, text, flags=flags)
            return text
        kept: list[str] = []
        for line in text.splitlines():
            candidate = line.strip()
            expected = rule.value.casefold() if rule.ignore_case else rule.value
            compared = candidate.casefold() if rule.ignore_case else candidate
            matched = (
                (rule.kind == "exact" and compared == expected)
                or (rule.kind == "contains" and expected in compared)
                or (rule.kind == "prefix" and compared.startswith(expected))
                or (rule.kind == "suffix" and compared.endswith(expected))
            )
            if matched:
                if rule.replacement:
                    kept.append(rule.replacement)
            else:
                kept.append(line)
        return "\n".join(kept)

    @staticmethod
    def _compare(left: str, right: str, flags: int) -> bool:
        return left.casefold() == right.casefold() if flags else left == right

    @staticmethod
    def _normalize(text: str) -> str:
        unique: list[str] = []
        seen: set[str] = set()
        for raw_line in text.splitlines():
            line = re.sub(r"\s+", " ", raw_line).strip(" /|:;-")
            if not line:
                continue
            identity = line.casefold()
            if identity not in seen:
                seen.add(identity)
                unique.append(line)
        return "\n".join(unique)

    @staticmethod
    def _validate_language(text: str, language: str) -> None:
        cyrillic = re.findall(r"[\u0400-\u052f]", text)
        latin = re.findall(r"[A-Za-z]", text)
        target = language.casefold()
        if target == "ru" and (not cyrillic or len(cyrillic) < len(latin)):
            raise FinalSanitationError("target language output is not Russian")
        if target == "en" and (
            not latin
            or cyrillic
            or re.search(r"[ÇĞİÖŞÜçğıöşü]", text) is not None
        ):
            raise FinalSanitationError("target language output is not English")
