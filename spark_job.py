import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.client import Config
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel, RandomForestClassifier
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import GBTRegressionModel, GBTRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

from dataops.config import load_env_config, storage_uri
from dataops.contract import contract_schema_fields, load_contract

try:
    from synapse.ml.isolationforest import IsolationForest
    from synapse.ml.isolationforest.IsolationForestModel import IsolationForestModel
except ImportError:  # pragma: no cover
    IsolationForest = None
    IsolationForestModel = None


ENV_CONFIG = load_env_config()
CONTRACT = load_contract()
RAW_PATH = os.getenv("LOGSTORM_RAW_PATH", storage_uri(ENV_CONFIG, "s3_raw", spark=True))
ENRICHED_PATH = os.getenv("LOGSTORM_ENRICHED_PATH", storage_uri(ENV_CONFIG, "s3_enriched", spark=True))
MODEL_BUCKET = os.getenv("LOGSTORM_MODEL_BUCKET", storage_uri(ENV_CONFIG, "s3_models").split("://", 1)[1])
DQ_REPORT_BUCKET = os.getenv("DQ_REPORT_BUCKET", storage_uri(ENV_CONFIG, "s3_dq_reports").split("://", 1)[1])
MODEL_PREFIX = os.getenv("LOGSTORM_MODEL_PREFIX", "v")
DDB_TABLE = os.getenv("LOGSTORM_DDB_TABLE", "logstorm-alerts")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "file:/tmp/mlruns")
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
DYNAMODB_ENDPOINT_URL = os.getenv("DYNAMODB_ENDPOINT_URL", "http://dynamodb-local:8000")
CHECKPOINT_PATH = os.getenv("LOGSTORM_CHECKPOINT_PATH", "/tmp/logstorm-checkpoints")
SAMPLE_RATE = float(os.getenv("LOGSTORM_SAMPLE_RATE", str(ENV_CONFIG.get("sample_rate", 1.0))))


@dataclass
class ModelBundle:
    version: int
    preprocessing: PipelineModel
    bot_model: Any
    latency_model: Any
    isolation_model: Any | None
    anomaly_mode: str
    rows_since_retrain: int


RAW_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("method", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", DoubleType(), True),
        StructField("user_id", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("country", StringType(), True),
        StructField("is_vpn", BooleanType(), True),
        StructField("pipeline_version", StringType(), True),
        StructField("processed_at", StringType(), True),
        StructField("lane", StringType(), True),
        StructField("is_bot", IntegerType(), True),
        StructField("rejection_reason", StringType(), True),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName(f"logstorm-ml-{ENV_CONFIG['name']}")
        .config("spark.cores.max", str(ENV_CONFIG.get("spark_cores", 1)))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )


def dynamodb_table():
    resource = boto3.resource(
        "dynamodb",
        endpoint_url=DYNAMODB_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )
    return resource.Table(DDB_TABLE)


def preprocessing_uri(version: int) -> str:
    return f"s3a://{MODEL_BUCKET}/{MODEL_PREFIX}{version}/preprocessing"


def bot_uri(version: int) -> str:
    return f"s3a://{MODEL_BUCKET}/{MODEL_PREFIX}{version}/bot_rf"


def latency_uri(version: int) -> str:
    return f"s3a://{MODEL_BUCKET}/{MODEL_PREFIX}{version}/latency_gbt"


def isolation_uri(version: int) -> str:
    return f"s3a://{MODEL_BUCKET}/{MODEL_PREFIX}{version}/isolation_forest"


def manifest_key(version: int) -> str:
    return f"{MODEL_PREFIX}{version}/manifest.json"


def list_versions() -> list[int]:
    seen: set[int] = set()
    for item in s3_client().list_objects_v2(Bucket=MODEL_BUCKET).get("Contents", []):
        prefix = item["Key"].split("/", 1)[0]
        if prefix.startswith(MODEL_PREFIX):
            try:
                seen.add(int(prefix.removeprefix(MODEL_PREFIX)))
            except ValueError:
                pass
    return sorted(seen)


def load_manifest(version: int) -> dict[str, Any]:
    obj = s3_client().get_object(Bucket=MODEL_BUCKET, Key=manifest_key(version))
    return json.loads(obj["Body"].read().decode("utf-8"))


def save_manifest(version: int, payload: dict[str, Any]) -> None:
    s3_client().put_object(
        Bucket=MODEL_BUCKET,
        Key=manifest_key(version),
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def latest_bundle() -> ModelBundle | None:
    versions = list_versions()
    if not versions:
        return None
    latest = versions[-1]
    manifest = load_manifest(latest)
    isolation_model = None
    if manifest["anomaly_mode"] == "synapseml_isolation_forest" and IsolationForestModel is not None:
        isolation_model = IsolationForestModel.load(isolation_uri(latest))
    return ModelBundle(
        version=latest,
        preprocessing=PipelineModel.load(preprocessing_uri(latest)),
        bot_model=RandomForestClassificationModel.load(bot_uri(latest)),
        latency_model=GBTRegressionModel.load(latency_uri(latest)),
        isolation_model=isolation_model,
        anomaly_mode=manifest["anomaly_mode"],
        rows_since_retrain=int(manifest.get("rows_since_retrain", 0)),
    )


def build_preprocessing_pipeline() -> Pipeline:
    return Pipeline(
        stages=[
            StringIndexer(inputCol="endpoint", outputCol="endpoint_idx", handleInvalid="keep"),
            StringIndexer(inputCol="http_method", outputCol="http_method_idx", handleInvalid="keep"),
            OneHotEncoder(
                inputCols=["endpoint_idx", "http_method_idx"],
                outputCols=["endpoint_ohe", "http_method_ohe"],
                handleInvalid="keep",
            ),
            VectorAssembler(
                inputCols=[
                    "hour_of_day",
                    "day_of_week",
                    "is_weekend",
                    "req_per_minute_per_ip",
                    "error_rate_5min",
                    "avg_latency_15min",
                    "endpoint_ohe",
                    "http_method_ohe",
                ],
                outputCol="features",
                handleInvalid="keep",
            ),
        ]
    )


def feature_engineering(df: DataFrame) -> DataFrame:
    sampled = df.sample(withReplacement=False, fraction=min(max(SAMPLE_RATE, 0.0), 1.0))
    normalized = (
        sampled.withColumn("event_time", F.to_timestamp("timestamp"))
        .filter(F.col("event_time").isNotNull())
        .withColumn("http_method", F.coalesce(F.col("method"), F.lit("UNKNOWN")))
        .withColumn("endpoint", F.coalesce(F.col("endpoint"), F.lit("/unknown")))
        .withColumn("status_code", F.col("status_code").cast(IntegerType()))
        .withColumn("response_time_ms", F.col("response_time_ms").cast(DoubleType()))
        .withColumn("is_bot", F.coalesce(F.col("is_bot"), F.lit(0)).cast(DoubleType()))
        .withColumn("hour_of_day", F.hour("event_time"))
        .withColumn("day_of_week", F.dayofweek("event_time"))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1.0).otherwise(0.0))
        .withColumn("is_server_error", F.when(F.col("status_code") >= 500, 1.0).otherwise(0.0))
    )
    watermarked = normalized.withWatermark("event_time", "10 minutes")
    rpm = watermarked.groupBy("ip", F.window("event_time", "1 minute")).count().withColumnRenamed("count", "req_per_minute_per_ip").alias("rpm")
    err = watermarked.groupBy("ip", F.window("event_time", "5 minutes")).agg(F.avg("is_server_error").alias("error_rate_5min")).alias("err")
    lat = watermarked.groupBy("ip", F.window("event_time", "15 minutes")).agg(F.avg("response_time_ms").alias("avg_latency_15min")).alias("lat")
    events = normalized.alias("events")
    return (
        events.join(
            rpm,
            (F.col("events.ip") == F.col("rpm.ip"))
            & (F.col("events.event_time") >= F.col("rpm.window.start"))
            & (F.col("events.event_time") < F.col("rpm.window.end")),
            "left",
        )
        .join(
            err,
            (F.col("events.ip") == F.col("err.ip"))
            & (F.col("events.event_time") >= F.col("err.window.start"))
            & (F.col("events.event_time") < F.col("err.window.end")),
            "left",
        )
        .join(
            lat,
            (F.col("events.ip") == F.col("lat.ip"))
            & (F.col("events.event_time") >= F.col("lat.window.start"))
            & (F.col("events.event_time") < F.col("lat.window.end")),
            "left",
        )
        .select("events.*", F.col("rpm.req_per_minute_per_ip"), F.col("err.error_rate_5min"), F.col("lat.avg_latency_15min"))
        .fillna({"req_per_minute_per_ip": 0.0, "error_rate_5min": 0.0, "avg_latency_15min": 0.0, "is_bot": 0.0})
    )


def save_model_metadata(version: int, rows_since_retrain: int, anomaly_mode: str, batch_size: int) -> None:
    import mlflow

    mlflow.set_tracking_uri(MLFLOW_URI)
    with mlflow.start_run(run_name=f"logstorm-model-v{version}"):
        mlflow.log_params(
            {
                "env": ENV_CONFIG["name"],
                "model_version": version,
                "anomaly_mode": anomaly_mode,
                "batch_training_rows": batch_size,
                "rows_since_retrain": rows_since_retrain,
            }
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as handle:
            json.dump(
                {
                    "version": version,
                    "env": ENV_CONFIG["name"],
                    "anomaly_mode": anomaly_mode,
                    "batch_training_rows": batch_size,
                    "rows_since_retrain": rows_since_retrain,
                },
                handle,
                indent=2,
            )
            temp_path = handle.name
        mlflow.log_artifact(temp_path, artifact_path="metadata")


def persist_models(bundle: ModelBundle, batch_size: int) -> ModelBundle:
    bundle.preprocessing.write().overwrite().save(preprocessing_uri(bundle.version))
    bundle.bot_model.write().overwrite().save(bot_uri(bundle.version))
    bundle.latency_model.write().overwrite().save(latency_uri(bundle.version))
    if bundle.isolation_model is not None:
        bundle.isolation_model.write().overwrite().save(isolation_uri(bundle.version))
    save_manifest(
        bundle.version,
        {
            "version": bundle.version,
            "anomaly_mode": bundle.anomaly_mode,
            "rows_since_retrain": bundle.rows_since_retrain,
            "batch_training_rows": batch_size,
        },
    )
    save_model_metadata(bundle.version, bundle.rows_since_retrain, bundle.anomaly_mode, batch_size)
    return bundle


def train_models(engineered: DataFrame, current: ModelBundle | None, batch_size: int) -> ModelBundle:
    next_version = 1 if current is None else current.version + 1
    preprocessing = build_preprocessing_pipeline().fit(engineered)
    prepared = preprocessing.transform(engineered)
    bot_model = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_bot",
        predictionCol="bot_prediction",
        probabilityCol="bot_probability",
        numTrees=60,
        maxDepth=8,
        seed=42,
    ).fit(prepared)
    latency_model = GBTRegressor(
        featuresCol="features",
        labelCol="response_time_ms",
        predictionCol="predicted_latency",
        maxIter=30,
        maxDepth=6,
        seed=42,
    ).fit(prepared)
    isolation_model = None
    anomaly_mode = "heuristic_proxy"
    if IsolationForest is not None:
        isolation_model = IsolationForest(
            featuresCol="features",
            predictionCol="if_prediction",
            scoreCol="if_raw_score",
            contamination=0.05,
            randomSeed=42,
            numEstimators=100,
        ).fit(prepared)
        anomaly_mode = "synapseml_isolation_forest"
    bundle = ModelBundle(next_version, preprocessing, bot_model, latency_model, isolation_model, anomaly_mode, 0)
    return persist_models(bundle, batch_size)


def ensure_bundle(engineered: DataFrame, batch_size: int) -> ModelBundle:
    current = latest_bundle()
    if current is None:
        return train_models(engineered, None, batch_size)
    rows_since_retrain = current.rows_since_retrain + batch_size
    if rows_since_retrain >= 500:
        return train_models(engineered, current, batch_size)
    current.rows_since_retrain = rows_since_retrain
    save_manifest(
        current.version,
        {
            "version": current.version,
            "anomaly_mode": current.anomaly_mode,
            "rows_since_retrain": current.rows_since_retrain,
            "batch_training_rows": 0,
        },
    )
    return current


def apply_models(bundle: ModelBundle, engineered: DataFrame) -> DataFrame:
    prepared = bundle.preprocessing.transform(engineered)
    scored = bundle.bot_model.transform(prepared)
    scored = bundle.latency_model.transform(scored)
    if bundle.isolation_model is not None:
        scored = bundle.isolation_model.transform(scored)
        scored = scored.withColumn("anomaly_score", F.when(F.col("if_raw_score").isNull(), F.lit(0.0)).otherwise(F.lit(1.0) - F.col("if_raw_score")))
    else:
        scored = scored.withColumn(
            "anomaly_score",
            F.least(
                F.lit(1.0),
                (
                    (F.col("req_per_minute_per_ip") / F.lit(50.0))
                    + (F.col("error_rate_5min") * F.lit(1.5))
                    + (F.col("avg_latency_15min") / F.lit(3000.0))
                )
                / F.lit(3.5),
            ),
        )
    return (
        scored.withColumn("bot_score", vector_to_array("bot_probability").getItem(1))
        .withColumn("is_bot_prediction", F.when(F.col("bot_score") >= 0.7, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("model_version", F.lit(bundle.version))
        .withColumn("anomaly_mode", F.lit(bundle.anomaly_mode))
        .drop("endpoint_idx", "http_method_idx", "endpoint_ohe", "http_method_ohe", "features")
    )


def ge_metrics(result: dict[str, Any]) -> tuple[int | None, float | None, float | None]:
    statistics = result.get("statistics", {})
    meta = result.get("meta", {}).get("custom_expectations", {})
    return (
        statistics.get("evaluated_expectations"),
        0.0 if statistics.get("unsuccessful_expectations") == 0 else float(statistics.get("unsuccessful_expectations", 0)) / max(float(statistics.get("evaluated_expectations", 1)), 1.0),
        None if meta.get("failing_row_count") is None else float(meta["failing_row_count"]),
    )


def write_alerts(scored: DataFrame) -> None:
    alerts = scored.filter(F.col("anomaly_score") > 0.8).collect()
    table = dynamodb_table()
    for row in alerts:
        item = row.asDict(recursive=True)
        item["alert_id"] = f"{item.get('user_id', 'unknown')}-{uuid.uuid4()}"
        for key in ["response_time_ms", "anomaly_score", "bot_score", "predicted_latency"]:
            if item.get(key) is not None:
                item[key] = str(float(item[key]))
        table.put_item(Item=item)


def foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
    from dataops.ge_runner import upload_validation_result, validate_batch
    from lineage.emit_lineage import emit_event
    import mlflow

    if batch_df.rdd.isEmpty():
        return
    run_id = str(uuid.uuid4())
    emit_event(
        job_name="spark.feature_engineering",
        run_id=run_id,
        event_type="START",
        inputs=[{"name": "logstorm-raw", "schema": contract_schema_fields(CONTRACT)}],
        outputs=[],
    )
    engineered = feature_engineering(batch_df).cache()
    batch_size = engineered.count()
    if batch_size == 0:
        engineered.unpersist()
        return
    bundle = ensure_bundle(engineered, batch_size)
    scored = apply_models(bundle, engineered).cache()
    scored.write.mode("append").parquet(ENRICHED_PATH)
    write_alerts(scored)

    pdf = scored.limit(50000).toPandas()
    if "error_anomaly_threshold" not in pdf.columns:
        pdf["error_anomaly_threshold"] = pdf["status_code"].apply(lambda value: 0.3 if value >= 400 else 0.0)
    validation = validate_batch(pdf)
    dq_report_key = upload_validation_result(validation)
    row_count_metric, null_metric, anomaly_metric = ge_metrics(validation)

    emit_event(
        job_name="spark.feature_engineering",
        run_id=run_id,
        event_type="COMPLETE",
        inputs=[{"name": "logstorm-raw", "schema": contract_schema_fields(CONTRACT), "row_count": batch_size}],
        outputs=[
            {
                "name": "logstorm-enriched",
                "schema": contract_schema_fields(CONTRACT) + [{"name": "anomaly_score", "type": "double"}, {"name": "bot_score", "type": "double"}],
                "row_count": batch_size,
                "null_percent": null_metric,
                "anomaly_rate": anomaly_metric,
            },
            {"name": "logstorm-alerts", "schema": [{"name": "alert_id", "type": "string"}]},
        ],
    )
    mlflow.set_tracking_uri(MLFLOW_URI)
    with mlflow.start_run(run_name=f"logstorm-batch-{batch_id}"):
        mlflow.log_params({"env": ENV_CONFIG["name"], "batch_id": batch_id, "model_version": bundle.version, "dq_report_key": dq_report_key})

    scored.unpersist()
    engineered.unpersist()


def main() -> None:
    spark = build_spark()
    source_df = spark.readStream.format("parquet").schema(RAW_SCHEMA).load(RAW_PATH)
    query = (
        source_df.writeStream.option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .foreachBatch(foreach_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
