from pathlib import Path

import pulsar
import pytest
from click.testing import CliRunner
from pytest_mock import MockerFixture

from billing_collector import __main__ as main
from billing_collector.pulsar_auth import pulsar_authentication


@pytest.fixture(autouse=True)
def clear_token_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PULSAR_TOKEN_FILE", raising=False)
    monkeypatch.delenv("PULSAR_TOKEN", raising=False)


def test_no_token_is_anonymous() -> None:
    assert pulsar_authentication() is None


def test_token_from_env(monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture) -> None:
    auth_token = mocker.patch("pulsar.AuthenticationToken")
    monkeypatch.setenv("PULSAR_TOKEN", " abc\n")

    assert pulsar_authentication() is auth_token.return_value
    auth_token.assert_called_once_with("abc")


def test_token_file_is_preferred_and_reread_on_every_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    auth_token = mocker.patch("pulsar.AuthenticationToken")
    token_file = tmp_path / "token"
    token_file.write_text("first\n")
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("PULSAR_TOKEN", "ignored")

    assert pulsar_authentication() is auth_token.return_value
    (supplier,) = auth_token.call_args.args
    assert supplier() == "first"

    token_file.write_text("second\n")
    assert supplier() == "second"


def test_token_file_supplier_returns_empty_token_when_file_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    auth_token = mocker.patch("pulsar.AuthenticationToken")
    token_file = tmp_path / "token"
    token_file.write_text("abc")
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(token_file))

    pulsar_authentication()
    (supplier,) = auth_token.call_args.args
    token_file.unlink()

    assert supplier() == ""
    assert "Could not read Pulsar token file" in caplog.text


def test_fails_at_startup_if_token_file_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(tmp_path / "missing"))

    with pytest.raises(FileNotFoundError):
        pulsar_authentication()


def test_fails_at_startup_if_token_file_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("\n")
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(token_file))

    with pytest.raises(ValueError, match="empty"):
        pulsar_authentication()


def test_real_authentication_token_accepts_file_supplier(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("abc")
    monkeypatch.setenv("PULSAR_TOKEN_FILE", str(token_file))

    assert isinstance(pulsar_authentication(), pulsar.AuthenticationToken)


def test_cli_passes_authentication_to_client(monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture) -> None:
    client = mocker.patch("pulsar.Client")
    mocker.patch.object(main, "ResourceUsageMessager")
    mocker.patch.object(main, "setup_logging")
    mocker.patch.object(main, "log_component_version")
    monkeypatch.setenv("PULSAR_TOKEN", "abc")

    result = CliRunner().invoke(main.cli, ["--pulsar-url", "pulsar://broker:6650"])

    assert result.exit_code == 0, result.output
    assert client.call_args.args == ("pulsar://broker:6650",)
    assert isinstance(client.call_args.kwargs["authentication"], pulsar.AuthenticationToken)
