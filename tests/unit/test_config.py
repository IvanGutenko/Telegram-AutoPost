import pytest
from pydantic import ValidationError

from emoda.config import Settings


def test_empty_optional_gemini_secret_is_treated_as_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "")
    assert Settings(_env_file=None).gemini_api_key is None


def test_empty_optional_telegram_api_id_is_treated_as_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TELEGRAM_API_ID", "")
    assert Settings(_env_file=None).telegram_api_id is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", []),
        ("123456789", [123456789]),
        ("123456789, 987654321", [123456789, 987654321]),
    ],
)
def test_bootstrap_admin_ids_accept_dotenv_syntax(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: list[int]
) -> None:
    monkeypatch.setenv("BOOTSTRAP_ADMIN_IDS", raw)
    assert Settings(_env_file=None).bootstrap_admin_ids == expected


def test_periodic_catchup_interval_is_bounded_and_defaults_to_thirty_seconds() -> None:
    assert Settings(_env_file=None).catchup_interval_seconds == 30
    with pytest.raises(ValidationError):
        Settings(_env_file=None, catchup_interval_seconds=4)
