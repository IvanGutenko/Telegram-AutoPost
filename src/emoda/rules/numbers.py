import re


def normalize_number(value: str) -> str:
    """Normalize one bounded numeric candidate, never an entire message.

    Refuse to join lines, ranges, currencies or multiple decimal separators.
    Horizontal Unicode spaces (including NBSP) are accepted inside a number.
    """
    stripped = value.strip()
    if re.fullmatch(r"[0-9](?:[0-9]|[^\S\r\n\v\f\x85\u2028\u2029])*"
                    r"(?:[.,][^\S\r\n\v\f\x85\u2028\u2029]*[0-9]"
                    r"(?:[0-9]|[^\S\r\n\v\f\x85\u2028\u2029])*)?", stripped):
        return re.sub(r"\s+", "", stripped).replace(",", ".")
    return stripped
