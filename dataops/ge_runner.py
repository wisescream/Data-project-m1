import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import great_expectations as gx
import pandas as pd
from botocore.client import Config
from great_expectations.core.expectation_suite import ExpectationSuite


SUITE_PATH = Path(os.getenv("GE_SUITE_PATH", "ge/logstorm_suite.json"))
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
DQ_REPORT_BUCKET = os.getenv("DQ_REPORT_BUCKET", "logstorm-dq-reports")
GE_DOCS_DIR = Path(os.getenv("GE_DOCS_DIR", "ge/data_docs"))


def suite() -> ExpectationSuite:
    payload = json.loads(SUITE_PATH.read_text(encoding="utf-8"))
    return ExpectationSuite(**payload)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )


def validate_batch(pdf: pd.DataFrame) -> dict[str, Any]:
    context = gx.get_context(mode="ephemeral")
    batch = context.data_sources.add_pandas("logstorm_pandas").read_dataframe(pdf, asset_name="logstorm_enriched")
    validation = batch.validate(expectation_suite=suite())

    # Great Expectations does not natively express the project-specific business rule, so we append it
    # as a custom result while keeping the suite as the primary contract.
    custom_rule_success = True
    failing_rows = pdf[(pdf["status_code"] >= 400) & (pdf["anomaly_score"] <= 0.3)] if not pdf.empty else pdf
    if not failing_rows.empty:
        custom_rule_success = False
    validation.describe_dict()["meta"] = {
        "custom_expectations": {
            "status_code_ge_400_implies_anomaly_score_gt_0_3": custom_rule_success,
            "failing_row_count": int(len(failing_rows)),
        }
    }

    GE_DOCS_DIR.mkdir(parents=True, exist_ok=True)
    index_path = GE_DOCS_DIR / "index.html"
    index_path.write_text(
        "<html><body><h1>LogStorm GE Data Docs</h1><p>Validation results are written to S3 and summarized per batch.</p></body></html>",
        encoding="utf-8",
    )
    return validation.describe_dict()


def upload_validation_result(result: dict[str, Any]) -> str:
    now = datetime.now(timezone.utc)
    key = f"{now:%Y/%m/%d/%H}/validation-{now:%Y%m%dT%H%M%S}.json"
    s3_client().put_object(
        Bucket=DQ_REPORT_BUCKET,
        Key=key,
        Body=json.dumps(result, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return key
