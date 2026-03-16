from datetime import datetime, timezone

import monitor


def test_list_objects_handles_single_page(mocker):
    fake_client = mocker.Mock()
    fake_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "one.parquet", "LastModified": datetime.now(timezone.utc), "Size": 10}],
        "IsTruncated": False,
    }
    mocker.patch("monitor.s3_client", return_value=fake_client)
    results = monitor.list_objects("logstorm-raw")
    assert len(results) == 1
    assert results[0]["Key"] == "one.parquet"


def test_contract_compliance_sets_metric_and_records_history(mocker):
    mocker.patch("monitor.objects_in_last_minutes", side_effect=[[{"Key": "raw1"}] * 100, [{"Key": "bad1"}]])
    mocker.patch("monitor.notify")
    state = {"history": {}}
    monitor.contract_compliance(state)
    assert state["history"]["contract_compliance"][-1]["success"] is True
    assert monitor.CONTRACT_COMPLIANCE._value.get() > 0.0
