# LogStorm Codebase Structure

## Runtime Entry Points

- `app.py`: FastAPI application that emits structured request logs
- `load_gen.py`: synthetic traffic generator for normal, bot, and chaos profiles
- `nifi_bootstrap.py`: bootstraps the layered NiFi canvas through the NiFi REST API
- `spark_job.py`: Structured Streaming + ML + GE + OpenLineage
- `monitor.py`: S3 monitoring daemon, SLO tracking, Prometheus metrics, and OpenLineage reads
- `dataops_service.py`: contract validation API and Prometheus exporter used by NiFi

## Domain Assets

- `contracts/`: OpenDataContract YAML and its CI validation schema
- `ge/`: Great Expectations suite and Data Docs web root
- `lineage/`: OpenLineage emission helpers
- `dataops/`: shared Python helpers for config, contracts, SLOs, and GE orchestration
- `docs/`: operator-facing documentation
- `tests/`: unit tests for generator, monitor, and Spark feature logic
- `.github/workflows/`: CI/CD workflow

## Infrastructure

- `docker/`: Dockerfiles for web, GeoIP, rsyslog, Spark, and NiFi bootstrap
- `docker-compose.yml`: local platform stack including NiFi, MinIO, Marquez, Prometheus, and Grafana
- `observability/`: Prometheus scrape config and Grafana provisioning

## Conventions

- Shared runtime behavior belongs in `dataops/` rather than being duplicated in entry-point scripts.
- All environment-sensitive paths resolve from `config.yml` with `ENV=dev|staging|prod`, then allow explicit env-var overrides for local compose.
- Contract, DQ, lineage, and monitoring assets live next to each other so DataOps changes remain reviewable as one slice.
