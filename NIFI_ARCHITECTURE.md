# LogStorm NiFi Architecture

## Canvas Layout

The NiFi root canvas should contain exactly one product-level Process Group:

- `LogStorm Platform`

Inside `LogStorm Platform`, the canvas must remain high-level and left-to-right only:

```text
+-------------+    +---------------+    +----------------------+    +---------------------+    +-----------+    +-------------------+    +-----------------------+
| 01 Sources  | -> | 02 Ingestion  | -> | 03 Data Quality      | -> | 04 Transformation   | -> | 05 Routing| -> | 06 Delivery        | -> | 07 System & Errors    |
|             |    |               |    | & Validation         |    | & Enrichment        |    |           |    | & Storage          |    |                       |
+-------------+    +---------------+    +----------------------+    +---------------------+    +-----------+    +-------------------+    +-----------------------+
```

Only Process Groups belong on this canvas. No processors, no controller services, and no long cross-canvas connections.

## Layered Design

### `01 Sources`

Responsibility:
- Receive logs and events from upstream producers.
- Normalize source-specific transport concerns before handoff to ingestion.

Processors:
- `ListenSyslog` for UDP 5140 intake from rsyslog
- Optional future sources:
  - `ListenHTTP`
  - `ConsumeKafka`
  - `ConsumeMQTT`

Ports:
- Output port: `raw_events`
- Output port: `source_failures`

Notes:
- This group should never perform enrichment or business routing.
- Source-specific parsers stay here only if required to expose a common raw payload.

### `02 Ingestion`

Responsibility:
- Convert transport-wrapped payloads into canonical raw event content.
- Preserve source metadata and make payload extraction deterministic.

Processors:
- `ExtractText` to isolate the JSON blob from the syslog envelope
- `ReplaceText` or `UpdateRecord` to restore canonical JSON content when needed
- `AttributesToJSON` if source metadata needs to be retained in payload

Ports:
- Input port: `raw_events`
- Output port: `canonical_events`
- Output port: `ingestion_failures`

Notes:
- The output from this group should already be canonical event JSON, not a syslog frame.

### `03 Data Quality & Validation`

Responsibility:
- Validate schema presence, required fields, and parseability.
- Drop, quarantine, or tag malformed records before transformation.

Processors:
- `EvaluateJsonPath` to extract required fields
- `RouteOnAttribute` for required-field presence checks
- `ValidateRecord` when a formal schema contract is introduced
- `DetectDuplicate` for idempotency and replay safety

Ports:
- Input port: `canonical_events`
- Output port: `validated_events`
- Output port: `duplicate_events`
- Output port: `invalid_events`

Validation rules:
- Required fields:
  - `timestamp`
  - `method`
  - `endpoint`
  - `status_code`
  - `response_time_ms`
  - `user_id`
  - `ip`
- Duplicate key:
  - `${user_id}_${timestamp}_${endpoint}`

### `04 Transformation & Enrichment`

Responsibility:
- Enrich validated events with reference data and operational metadata.
- Convert attribute-level enrichment into the canonical payload.

Processors:
- `ReplaceText` to prepare GeoIP request body
- `InvokeHTTP` to local GeoIP service
- `UpdateAttribute` to add:
  - `pipeline.version`
  - `processed.at`
  - `country`
  - `is_vpn`
- `ReplaceText` to build the enriched canonical JSON event

Ports:
- Input port: `validated_events`
- Output port: `enriched_events`
- Output port: `enrichment_failures`

Notes:
- Any external lookup belongs here, not in ingestion.
- Keep request/response shaping local to this group.

### `05 Routing`

Responsibility:
- Classify events into operational lanes for downstream handling.
- Keep routing policy explicit and isolated from transformation logic.

Processors:
- `RouteOnAttribute`

Routing rules:
- `error_lane`: `${status_code:matches('4\\d\\d|5\\d\\d')}`
- `slow_lane`: `${response_time_ms:toNumber():gt(1000)}`
- `normal_lane`: everything else

Ports:
- Input port: `enriched_events`
- Output port: `normal_events`
- Output port: `slow_events`
- Output port: `error_events`

Notes:
- Routing groups should not contain format conversion or storage processors.

### `06 Delivery & Storage`

Responsibility:
- Convert records into storage formats and deliver to sinks.
- Keep storage-specific concerns isolated and replaceable.

Processors:
- `ConvertRecord` using:
  - `JsonTreeReader`
  - `ParquetRecordSetWriter`
- `PutS3Object` to `logstorm-raw`

Storage pattern:
- Bucket: `logstorm-raw`
- Key:
  - `year=${now():format('yyyy')}/month=${now():format('MM')}/day=${now():format('dd')}/hour=${now():format('HH')}/file-${uuid()}.parquet`

Ports:
- Input port: `normal_events`
- Input port: `slow_events`
- Input port: `error_events`
- Output port: `storage_success`
- Output port: `storage_failures`

Notes:
- If different retention or bucket policies are needed later, split delivery into:
  - `06A Raw Delivery`
  - `06B Error Delivery`
  - `06C Curated Delivery`

### `07 System & Errors`

Responsibility:
- Centralize retries, dead-letter queues, diagnostics, and observability.
- Keep every failure path visible and explicit.

Processors:
- `MergeRecord` or `MergeContent` for error batching if needed
- `PutFile` or `PutS3Object` for DLQ storage
- `LogMessage` or `PublishKafka` for operational alerts
- Optional `RetryFlowFile` patterns where safe

Ports:
- Input port: `source_failures`
- Input port: `ingestion_failures`
- Input port: `invalid_events`
- Input port: `duplicate_events`
- Input port: `enrichment_failures`
- Input port: `storage_failures`

Notes:
- Every Process Group should route failures here, never to hidden auto-termination by default unless the relationship is operationally irrelevant.

## Recommended Internal Wiring

```text
01 Sources/raw_events
  -> 02 Ingestion/canonical_events
  -> 03 Data Quality & Validation/validated_events
  -> 04 Transformation & Enrichment/enriched_events
  -> 05 Routing/{normal_events,slow_events,error_events}
  -> 06 Delivery & Storage/storage_success

Any failure relationship
  -> 07 System & Errors
```

## Naming Conventions

Use numbered Process Groups and human-readable names:

- `01 Sources`
- `02 Ingestion`
- `03 Data Quality & Validation`
- `04 Transformation & Enrichment`
- `05 Routing`
- `06 Delivery & Storage`
- `07 System & Errors`

Processor naming rules:

- Use verb + subject:
  - `Extract Syslog JSON`
  - `Validate Required Fields`
  - `Fetch GeoIP`
  - `Convert JSON To Parquet`
  - `Write Raw Events To S3`
- Avoid generic names like `UpdateAttribute1` or `RouteOnAttribute2`.

Port naming rules:

- Inputs:
  - `raw_events`
  - `validated_events`
  - `error_events`
- Outputs:
  - `canonical_events`
  - `enriched_events`
  - `storage_failures`

## Controller Service Strategy

Keep controller services scoped at the `LogStorm Platform` Process Group level when they are shared:

- `AvroSchemaRegistry`
- `JsonTreeReader`
- `ParquetRecordSetWriter`
- `DistributedMapCacheServer`
- `DistributedMapCacheClientService`

This keeps shared schemas and dedupe services reusable without putting them on the root canvas.

## Maintenance Practices

### Canvas hygiene

- One flow direction only: left to right.
- No long backtracking connections.
- Use labels for major business rules only, not for every processor.
- Keep queue lines short and readable.

### Reusability

- Treat each Process Group as a reusable stage with stable input/output contracts.
- Add new sources by extending `01 Sources`, not by injecting processors into downstream groups.
- Add new sinks by extending `06 Delivery & Storage`, not by branching from transformation groups.

### Change management

- Version Process Groups in NiFi Registry once the flow stabilizes.
- Tag controller-service changes explicitly because they affect multiple groups.
- Introduce parameters for:
  - bucket names
  - ports
  - endpoint URLs
  - retry thresholds
  - environment-specific credentials

### Error handling

- Prefer explicit failure connections over silent auto-termination.
- Auto-terminate only truly disposable relationships like `invalid` when they are intentionally ignored.
- Preserve invalid payloads in a DLQ path with provenance-friendly metadata.

### Scalability

- Separate logical lanes before storage if volume grows:
  - normal lane
  - slow lane
  - error lane
- Split enrichment into its own dedicated group early if more lookup services are added.
- Introduce dedicated Process Groups for:
  - schema evolution
  - reference-data enrichment
  - alert fanout

## Growth Path

As the project grows, the next professional split would be:

- `04A Transformation`
- `04B Reference Enrichment`
- `05A Operational Routing`
- `05B Analytics Routing`
- `06A Raw S3 Delivery`
- `06B Alert / DLQ Delivery`

That keeps the pipeline readable even after multiple sinks, multiple sources, and more complex validation rules are added.
