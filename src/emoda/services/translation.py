from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Protocol


class TranslationAdapter(Protocol):
    async def translate(
        self, text: str, target_language: str, instruction: str = ""
    ) -> str: ...


class TranslationIntegrityError(ValueError):
    pass


class TranslationLanguageError(ValueError):
    pass


class ProtectedTranslator:
    def __init__(self, adapter: TranslationAdapter) -> None:
        self.adapter = adapter

    async def translate(
        self,
        text: str,
        target_language: str,
        protected_values: list[str],
        *,
        instruction: str = "",
    ) -> str:
        protected = text
        mapping: dict[str, str] = {}
        automatic_patterns = (
            r"(?<!\w)[$€]\s*\d+(?:[.,]\d+)?",
            r"\b\d+(?:[.,]\d+)?%",
            r"(?<!\w)\d+(?:[.,]\d+)?(?!\w)",
            r"\b(?=[A-Za-z0-9]*[A-Za-z])(?=[A-Za-z0-9]*\d)[A-Za-z0-9]+\b",
            r"\b(?=[A-Z0-9/-]*[A-Z])(?=[A-Z0-9/-]*\d)[A-Z0-9]+(?:[-/][A-Z0-9]+)+\b",
            r"\b(?:XXS|XS|S|M|L|XL|XXL|STD)\b",
        )
        automatic = {
            match.group(0) for pattern in automatic_patterns for match in re.finditer(pattern, text)
        }
        unique = sorted(
            {value for value in [*protected_values, *automatic] if value},
            key=len,
            reverse=True,
        )
        token_by_value: dict[str, str] = {}
        token_index = 0
        for value in unique:
            if value not in text:
                continue
            token = f"__P{token_index}__"
            while token in text or token in mapping:
                token_index += 1
                token = f"__P{token_index}__"
            token_index += 1
            token_by_value[value] = token
            mapping[token] = value
        if token_by_value:
            alternatives = "|".join(re.escape(value) for value in token_by_value)
            used_tokens: set[str] = set()

            def replace(match: re.Match[str]) -> str:
                token = token_by_value[match.group(0)]
                used_tokens.add(token)
                return token

            protected = re.sub(
                alternatives,
                replace,
                text,
            )
            mapping = {token: value for token, value in mapping.items() if token in used_tokens}
        translated = await self.adapter.translate(
            protected, target_language, instruction=instruction
        )
        missing = [token for token in mapping if token not in translated]
        if missing:
            raise TranslationIntegrityError(f"translation lost protected placeholders: {missing}")
        self._validate_target_language(protected, translated, target_language)
        for token, value in mapping.items():
            translated = translated.replace(token, value)
        for value in mapping.values():
            if value not in translated:
                raise TranslationIntegrityError(f"translation changed protected value: {value}")
        return translated

    @staticmethod
    def _validate_target_language(source: str, translated: str, target_language: str) -> None:
        source_visible = re.sub(r"__P\d+__", "", source)
        translated_visible = re.sub(r"__P\d+__", "", translated)
        if not any(character.isalpha() for character in source_visible):
            return
        target = target_language.upper()
        cyrillic = re.findall(r"[\u0400-\u052f]", translated_visible)
        latin = re.findall(r"[A-Za-z]", translated_visible)
        if target == "RU" and (not cyrillic or len(cyrillic) < len(latin)):
            raise TranslationLanguageError(
                "RU target language validation failed: translated description is not Russian"
            )
        if target == "RU" and ProtectedTranslator._looks_like_transliteration(
            source_visible, translated_visible
        ):
            raise TranslationLanguageError(
                "RU target language validation failed: output is source transliteration"
            )
        if target == "EN" and (
            not latin
            or cyrillic
            or re.search(r"[ÇĞİÖŞÜçğıöşü]", translated_visible) is not None
        ):
            raise TranslationLanguageError(
                "EN target language validation failed: translated description is not English"
            )

    @staticmethod
    def _looks_like_transliteration(source: str, translated: str) -> bool:
        source_words = re.findall(r"[A-Za-z]{3,}", source.casefold())
        if len(source_words) < 3:
            return False
        cyrillic_to_latin = str.maketrans(
            {
                "а": "a",
                "б": "b",
                "в": "v",
                "г": "g",
                "д": "d",
                "е": "e",
                "ё": "e",
                "ж": "zh",
                "з": "z",
                "и": "i",
                "й": "i",
                "к": "k",
                "л": "l",
                "м": "m",
                "н": "n",
                "о": "o",
                "п": "p",
                "р": "r",
                "с": "s",
                "т": "t",
                "у": "u",
                "ф": "f",
                "х": "h",
                "ц": "ts",
                "ч": "ch",
                "ш": "sh",
                "щ": "shch",
                "ъ": "",
                "ы": "y",
                "ь": "",
                "э": "e",
                "ю": "yu",
                "я": "ya",
            }
        )
        translated_words = [
            word.translate(cyrillic_to_latin)
            for word in re.findall(r"[A-Za-z\u0400-\u052f]{3,}", translated.casefold())
        ]
        matched = sum(
            1
            for source_word in source_words
            if any(
                SequenceMatcher(None, source_word, translated_word).ratio() >= 0.78
                for translated_word in translated_words
            )
        )
        return matched >= 3 and matched / len(source_words) >= 0.6
