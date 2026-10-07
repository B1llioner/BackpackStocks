import json
import logging
from unittest.mock import Mock

from backpack_stocks.observability import LOGGER, JsonFormatter, event


def test_coverage_metrics_survive_the_private_log_filter_without_exposing_requests(monkeypatch):
    log = Mock()
    monkeypatch.setattr(LOGGER, "log", log)
    event(
        "coverage_frame",
        observed_at=1234.5,
        eligible=72,
        fresh=54,
        refused=15,
        known_blocks=3,
        waiting_bank=0,
        waiting_assets="WEN.US,AMD.US",
        first_pass_completed=72,
        snapshot_ms=4.2,
        headers={"Authorization": "private-test-value"},
        secret="private-test-value",
        request_url="https://invalid.test/private-test-value",
    )
    public = log.call_args.kwargs["extra"]["public_fields"]
    record = logging.LogRecord(LOGGER.name, logging.INFO, __file__, 1, "coverage_frame", (), None)
    record.public_fields = public
    rendered = JsonFormatter().format(record)
    saved = json.loads(rendered)
    assert saved["fresh"] == 54 and saved["waiting_assets"] == "WEN.US,AMD.US"
    assert saved["observed_at"] == 1234.5 and saved["first_pass_completed"] == 72
    assert "private-test-value" not in rendered
    assert not {"headers", "secret", "request_url"} & saved.keys()
