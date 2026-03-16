# LogStorm

LogStorm is a school data engineering platform that ingests structured app logs through `rsyslog -> NiFi -> Spark -> S3`, then layers DataOps controls on top: contracts, CI/CD, Great Expectations, OpenLineage, environment promotion, and SLO monitoring.

## Structure

- Runtime apps:
  - [app.py](/C:/Users/Rayane/OneDrive/Bureau/Algo-/app.py)
  - [load_gen.py](/C:/Users/Rayane/OneDrive/Bureau/Algo-/load_gen.py)
  - [nifi_bootstrap.py](/C:/Users/Rayane/OneDrive/Bureau/Algo-/nifi_bootstrap.py)
  - [spark_job.py](/C:/Users/Rayane/OneDrive/Bureau/Algo-/spark_job.py)
  - [monitor.py](/C:/Users/Rayane/OneDrive/Bureau/Algo-/monitor.py)
  - [dataops_service.py](/C:/Users/Rayane/OneDrive/Bureau/Algo-/dataops_service.py)
- Shared DataOps assets:
  - [contracts/logstorm-log-event.yml](/C:/Users/Rayane/OneDrive/Bureau/Algo-/contracts/logstorm-log-event.yml)
  - [ge/logstorm_suite.json](/C:/Users/Rayane/OneDrive/Bureau/Algo-/ge/logstorm_suite.json)
  - [config.yml](/C:/Users/Rayane/OneDrive/Bureau/Algo-/config.yml)
  - [slo.yml](/C:/Users/Rayane/OneDrive/Bureau/Algo-/slo.yml)
  - [lineage/emit_lineage.py](/C:/Users/Rayane/OneDrive/Bureau/Algo-/lineage/emit_lineage.py)
- More detail:
  - [docs/PROJECT_STRUCTURE.md](/C:/Users/Rayane/OneDrive/Bureau/Algo-/docs/PROJECT_STRUCTURE.md)
  - [docs/DATAOPS.md](/C:/Users/Rayane/OneDrive/Bureau/Algo-/docs/DATAOPS.md)

## Local Boot

Bring up ingest plus DataOps services:

```powershell
docker compose up --build minio minio-init dynamodb-local dynamodb-init web geoip dataops-api postgres marquez nifi nifi-bootstrap rsyslog
```

Generate traffic:

```powershell
python load_gen.py --base-url http://localhost:8000 --duration 180
```

Start downstream processing:

```powershell
docker compose --profile ml up --build spark
docker compose --profile monitor up --build monitor ge-data-docs prometheus grafana
```

## Local Endpoints

- App: `http://localhost:8000/`
- GeoIP: `http://localhost:8081/health`
- DataOps API: `http://localhost:8090/health`
- NiFi: `http://localhost:8080/nifi`
- MinIO: `http://localhost:9001`
- Marquez: `http://localhost:5000`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- GE Data Docs: `http://localhost:8082`

## CI/CD

The CI/CD pipeline is defined in [.github/workflows/pipeline.yml](/C:/Users/Rayane/OneDrive/Bureau/Algo-/.github/workflows/pipeline.yml). It runs linting, unit tests, integration tests against Docker Compose, then deploy actions on merge to `main`.

## NiFi

The primary NiFi provisioning path is [nifi_bootstrap.py](/C:/Users/Rayane/OneDrive/Bureau/Algo-/nifi_bootstrap.py). It now includes:

- layered process groups
- contract validation through the Python DataOps API
- GeoIP enrichment
- routing lanes
- Parquet conversion
- raw delivery through the local S3 gateway
