# LogStorm DataOps Layer

## Data Contracts

- Contract source: `contracts/logstorm-log-event.yml`
- Validator runtime: `dataops_service.py`
- NiFi integration: `nifi_bootstrap.py` adds `Validate Contract (Python)` in the quality layer
- Contract failures:
  - increment `logstorm_contract_violations_total{field,reason}`
  - are written to the quarantine bucket with `rejection_reason`

## Great Expectations

- Suite: `ge/logstorm_suite.json`
- Spark job runs GE after each batch on the enriched frame
- Validation result JSON is written to the configured DQ reports bucket
- Data Docs are served from `ge/data_docs/` and exposed through Grafana iframe

## OpenLineage

- Helper: `lineage/emit_lineage.py`
- Backend: Marquez on `http://localhost:5000`
- Emitted by:
  - contract validation / quarantine path
  - Spark batch feature engineering and outputs
  - monitor daemon reads

## Environments

- Source of truth: `config.yml`
- Scripts read `ENV`
- Local compose overrides some storage targets to keep compatibility with the existing local ingest stack

## Observability

- Prometheus metrics:
  - contract violation totals
  - contract compliance ratio
  - SLO error budget remaining
  - burn-rate indicators
- Grafana:
  - Prometheus datasource is provisioned
  - GE Data Docs are embedded through an iframe panel

## Local Startup

```powershell
docker compose up --build minio minio-init dynamodb-local dynamodb-init web geoip dataops-api postgres marquez nifi nifi-bootstrap rsyslog
docker compose --profile ml up --build spark
docker compose --profile monitor up --build monitor ge-data-docs prometheus grafana
```
