"""FXMacroData calendar helper tests, with HTTP mocked."""
import pytest
import requests

from tradingstrategy.alternative_data import fxmacrodata


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


def test_fxmacrodata_calendar_does_not_follow_redirects(monkeypatch):
    captured = {}

    def fake_get(url, **kwargs):
        captured.update(kwargs)
        return FakeResponse({}, status_code=302)

    monkeypatch.setattr(fxmacrodata.requests, "get", fake_get)
    with pytest.raises(fxmacrodata.FXMacroDataError, match="HTTP 302"):
        fxmacrodata.fetch_fxmacrodata_calendar("usd", api_key="test-key")
    assert captured["allow_redirects"] is False
    assert captured["headers"] == {"X-API-Key": "test-key"}


def test_fxmacrodata_calendar_errors_do_not_include_the_key(monkeypatch):
    def fake_get(url, **kwargs):
        raise requests.ConnectionError(f"failed with {kwargs['headers']}")

    monkeypatch.setattr(fxmacrodata.requests, "get", fake_get)
    with pytest.raises(fxmacrodata.FXMacroDataError) as excinfo:
        fxmacrodata.fetch_fxmacrodata_calendar("usd", api_key="test-key")
    assert "test-key" not in str(excinfo.value)

    with pytest.raises(fxmacrodata.FXMacroDataError) as excinfo:
        fxmacrodata.fetch_fxmacrodata_calendar("usd", api_key="test\nkey")
    assert "test" not in str(excinfo.value)


@pytest.mark.parametrize("payload", [{"detail": "Invalid API key"}, [1, 2], {"data": "oops"}, None])
def test_fxmacrodata_calendar_rejects_malformed_payloads(monkeypatch, payload):
    monkeypatch.setattr(fxmacrodata.requests, "get", lambda url, **kwargs: FakeResponse(payload))
    with pytest.raises(fxmacrodata.FXMacroDataError, match="unexpected response"):
        fxmacrodata.fetch_fxmacrodata_calendar("usd")
