import os
import uuid
from datetime import datetime, timezone
from typing import Any

from openlineage.client import OpenLineageClient
from openlineage.client.facet import BaseFacet
from openlineage.client.facet import SchemaDatasetFacet, SchemaField
from openlineage.client.run import Dataset, InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.transport import HttpConfig, HttpTransport


MARQUEZ_URL = os.getenv("OPENLINEAGE_URL", "http://localhost:5000/api/v1/lineage")
NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "logstorm")


class DataQualityMetricsInputDatasetFacet(BaseFacet):
    _namespace = "logstorm"
    _name = "dataQualityMetrics"

    def __init__(self, row_count: int | None = None, null_percent: float | None = None, anomaly_rate: float | None = None):
        self.row_count = row_count
        self.null_percent = null_percent
        self.anomaly_rate = anomaly_rate


def client() -> OpenLineageClient:
    return OpenLineageClient(transport=HttpTransport(HttpConfig(url=MARQUEZ_URL)))


def dataset_schema_facet(fields: list[dict[str, str]]) -> SchemaDatasetFacet:
    return SchemaDatasetFacet(fields=[SchemaField(name=item["name"], type=item["type"]) for item in fields])


def emit_event(
    job_name: str,
    run_id: str | None,
    event_type: str,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
) -> str:
    resolved_run_id = run_id or str(uuid.uuid4())
    run_state = RunState.START if event_type == "START" else RunState.COMPLETE
    event = RunEvent(
        eventType=run_state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=resolved_run_id),
        job=Job(namespace=NAMESPACE, name=job_name),
        inputs=[
            InputDataset(
                namespace=NAMESPACE,
                name=item["name"],
                facets={
                    "schema": dataset_schema_facet(item.get("schema", [])),
                    "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                        row_count=item.get("row_count"),
                        null_percent=item.get("null_percent"),
                        anomaly_rate=item.get("anomaly_rate"),
                    ),
                },
            )
            for item in inputs
        ],
        outputs=[
            OutputDataset(
                namespace=NAMESPACE,
                name=item["name"],
                facets={
                    "schema": dataset_schema_facet(item.get("schema", [])),
                    "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                        row_count=item.get("row_count"),
                        null_percent=item.get("null_percent"),
                        anomaly_rate=item.get("anomaly_rate"),
                    ),
                },
            )
            for item in outputs
        ],
    )
    client().emit(event)
    return resolved_run_id
