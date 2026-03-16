import os
import time
from typing import Any

import requests


NIFI_URL = os.getenv("NIFI_URL", "http://localhost:8080").rstrip("/")
NIFI_USERNAME = os.getenv("NIFI_USERNAME", "admin")
NIFI_PASSWORD = os.getenv("NIFI_PASSWORD", "logstorm123456")
GEOIP_URL = os.getenv("GEOIP_URL", "http://geoip:8081/lookup")
S3_GATEWAY_URL = os.getenv("S3_GATEWAY_URL", "http://geoip:8081/s3/object")
DATAOPS_CONTRACT_URL = os.getenv("DATAOPS_CONTRACT_URL", "http://dataops-api:8090/contracts/validate")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

PLATFORM_GROUP_NAME = "LogStorm Platform"

GROUP_LAYOUT = {
    "01 Sources": (40.0, 120.0),
    "02 Ingestion": (340.0, 120.0),
    "03 Data Quality & Validation": (640.0, 120.0),
    "04 Transformation & Enrichment": (980.0, 120.0),
    "05 Routing": (1340.0, 120.0),
    "06 Delivery & Storage": (1640.0, 120.0),
    "07 System & Errors": (1940.0, 120.0),
}

LOGSTORM_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "logstorm_event",
  "namespace": "logstorm",
  "fields": [
    {"name": "timestamp", "type": ["null", "string"], "default": null},
    {"name": "method", "type": ["null", "string"], "default": null},
    {"name": "endpoint", "type": ["null", "string"], "default": null},
    {"name": "status_code", "type": ["null", "int"], "default": null},
    {"name": "response_time_ms", "type": ["null", "double"], "default": null},
    {"name": "user_id", "type": ["null", "string"], "default": null},
    {"name": "ip", "type": ["null", "string"], "default": null},
    {"name": "session_id", "type": ["null", "string"], "default": null},
    {"name": "user_agent", "type": ["null", "string"], "default": null},
    {"name": "country", "type": ["null", "string"], "default": null},
    {"name": "is_vpn", "type": ["null", "boolean"], "default": null},
    {"name": "pipeline_version", "type": ["null", "string"], "default": null},
    {"name": "processed_at", "type": ["null", "string"], "default": null},
    {"name": "lane", "type": ["null", "string"], "default": null},
    {"name": "is_bot", "type": ["null", "int"], "default": null}
  ]
}
""".strip()


def api(session: requests.Session, method: str, path: str, **kwargs) -> Any:
    response = session.request(method, f"{NIFI_URL}{path}", timeout=30, **kwargs)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise requests.HTTPError(f"{exc} :: {response.text}") from exc
    if response.content:
        return response.json()
    return {}


def wait_for_nifi(session: requests.Session) -> None:
    for _ in range(90):
        try:
            api(session, "GET", "/nifi-api/system-diagnostics")
            return
        except requests.RequestException:
            time.sleep(5)
    raise RuntimeError("NiFi did not become ready in time")


def login(session: requests.Session) -> None:
    config = api(session, "GET", "/nifi-api/access/config")
    if not config.get("config", {}).get("supportsLogin", False):
        return
    response = session.post(
        f"{NIFI_URL}/nifi-api/access/token",
        data={"username": NIFI_USERNAME, "password": NIFI_PASSWORD},
        timeout=30,
    )
    response.raise_for_status()
    session.headers.update({"Authorization": f"Bearer {response.text}"})


def root_group_id(session: requests.Session) -> str:
    return api(session, "GET", "/nifi-api/flow/process-groups/root")["processGroupFlow"]["id"]


def get_flow(session: requests.Session, process_group_id: str) -> dict[str, Any]:
    return api(session, "GET", f"/nifi-api/flow/process-groups/{process_group_id}")


def get_or_create_process_group(session: requests.Session, parent_group_id: str, name: str, position: tuple[float, float]) -> str:
    flow = get_flow(session, parent_group_id)
    for group in flow["processGroupFlow"]["flow"].get("processGroups", []):
        if group["component"]["name"] == name:
            return group["component"]["id"]
    created = api(
        session,
        "POST",
        f"/nifi-api/process-groups/{parent_group_id}/process-groups",
        json={
            "revision": {"version": 0},
            "component": {
                "name": name,
                "position": {"x": position[0], "y": position[1]},
            },
        },
    )
    return created["id"]


def get_port_by_name(session: requests.Session, process_group_id: str, name: str, port_kind: str) -> str | None:
    flow = get_flow(session, process_group_id)["processGroupFlow"]["flow"]
    collection = "inputPorts" if port_kind == "input" else "outputPorts"
    for port in flow.get(collection, []):
        if port["component"]["name"] == name:
            return port["component"]["id"]
    return None


def create_port(session: requests.Session, process_group_id: str, name: str, port_kind: str, position: tuple[float, float]) -> str:
    existing = get_port_by_name(session, process_group_id, name, port_kind)
    if existing:
        return existing
    endpoint = "input-ports" if port_kind == "input" else "output-ports"
    created = api(
        session,
        "POST",
        f"/nifi-api/process-groups/{process_group_id}/{endpoint}",
        json={
            "revision": {"version": 0},
            "component": {
                "name": name,
                "position": {"x": position[0], "y": position[1]},
            },
        },
    )
    return created["id"]


def get_processor_by_name(session: requests.Session, process_group_id: str, name: str) -> str | None:
    for proc in get_flow(session, process_group_id)["processGroupFlow"]["flow"].get("processors", []):
        if proc["component"]["name"] == name:
            return proc["component"]["id"]
    return None


def create_processor(
    session: requests.Session,
    process_group_id: str,
    name: str,
    proc_type: str,
    position: tuple[float, float],
    properties: dict[str, str],
) -> str:
    existing = get_processor_by_name(session, process_group_id, name)
    if existing:
        return existing
    created = api(
        session,
        "POST",
        f"/nifi-api/process-groups/{process_group_id}/processors",
        json={
            "revision": {"version": 0},
            "component": {
                "name": name,
                "type": proc_type,
                "position": {"x": position[0], "y": position[1]},
                "config": {"properties": properties},
            },
        },
    )
    return created["id"]


def get_controller_service_by_name(session: requests.Session, process_group_id: str, name: str) -> str | None:
    services = api(session, "GET", f"/nifi-api/flow/process-groups/{process_group_id}/controller-services")
    for service in services.get("controllerServices", []):
        if service["component"]["name"] == name:
            return service["component"]["id"]
    return None


def create_controller_service(
    session: requests.Session,
    process_group_id: str,
    name: str,
    service_type: str,
    properties: dict[str, str],
) -> str:
    existing = get_controller_service_by_name(session, process_group_id, name)
    if existing:
        return existing
    created = api(
        session,
        "POST",
        f"/nifi-api/process-groups/{process_group_id}/controller-services",
        json={
            "revision": {"version": 0},
            "component": {"name": name, "type": service_type, "properties": properties},
        },
    )
    return created["id"]


def enable_controller_service(session: requests.Session, service_id: str) -> None:
    entity = api(session, "GET", f"/nifi-api/controller-services/{service_id}")
    if entity["component"]["state"] == "ENABLED":
        return
    api(
        session,
        "PUT",
        f"/nifi-api/controller-services/{service_id}/run-status",
        json={
            "revision": entity["revision"],
            "state": "ENABLED",
            "disconnectedNodeAcknowledged": False,
        },
    )
    for _ in range(30):
        current = api(session, "GET", f"/nifi-api/controller-services/{service_id}")
        if current["component"]["state"] == "ENABLED":
            return
        time.sleep(2)
    raise RuntimeError(f"Controller service {service_id} did not enable in time")


def resolve_component(session: requests.Session, component_id: str) -> dict[str, Any]:
    lookups = [
        (f"/nifi-api/processors/{component_id}", "PROCESSOR"),
        (f"/nifi-api/input-ports/{component_id}", "INPUT_PORT"),
        (f"/nifi-api/output-ports/{component_id}", "OUTPUT_PORT"),
    ]
    for path, comp_type in lookups:
        try:
            entity = api(session, "GET", path)
            comp = entity["component"]
            return {"id": comp["id"], "type": comp_type, "groupId": comp["parentGroupId"], "name": comp["name"]}
        except requests.HTTPError:
            continue
    raise KeyError(f"Unable to resolve component {component_id}")


def create_connection(session: requests.Session, parent_group_id: str, source_id: str, destination_id: str, relationships: list[str]) -> None:
    flow = get_flow(session, parent_group_id)["processGroupFlow"]["flow"]
    for connection in flow.get("connections", []):
        comp = connection["component"]
        if comp["source"]["id"] == source_id and comp["destination"]["id"] == destination_id:
            # Keep bootstrap reruns non-destructive. Clean environments are recreated with
            # `docker compose down -v`, and NiFi rejects destination changes on running connections.
            return
    source = resolve_component(session, source_id)
    destination = resolve_component(session, destination_id)
    api(
        session,
        "POST",
        f"/nifi-api/process-groups/{parent_group_id}/connections",
        json={
            "revision": {"version": 0},
            "component": {
                "parentGroupId": parent_group_id,
                "source": source,
                "destination": destination,
                "selectedRelationships": relationships,
                "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB",
                "flowFileExpiration": "0 sec",
                "bends": [],
            },
        },
    )


def update_processor(session: requests.Session, processor_id: str, properties: dict[str, str], auto_terminated: list[str] | None = None) -> None:
    entity = api(session, "GET", f"/nifi-api/processors/{processor_id}")
    if entity["component"]["state"] in {"RUNNING", "STARTING"}:
        return
    config = entity["component"]["config"]
    config["properties"].update(properties)
    if auto_terminated is not None:
        config["autoTerminatedRelationships"] = auto_terminated
    api(
        session,
        "PUT",
        f"/nifi-api/processors/{processor_id}",
        json={"revision": entity["revision"], "component": {"id": processor_id, "config": config}},
    )


def start_processor(session: requests.Session, processor_id: str) -> None:
    entity = api(session, "GET", f"/nifi-api/processors/{processor_id}")
    if entity["component"]["state"] in {"RUNNING", "STARTING"}:
        return
    try:
        api(
            session,
            "PUT",
            f"/nifi-api/processors/{processor_id}/run-status",
            json={
                "revision": entity["revision"],
                "state": "RUNNING",
                "disconnectedNodeAcknowledged": False,
            },
        )
    except requests.HTTPError as exc:
        if "Current state is STARTING" in str(exc) or "Current state is RUNNING" in str(exc):
            return
        raise


def start_port(session: requests.Session, port_id: str, port_kind: str) -> None:
    endpoint = "input-ports" if port_kind == "input" else "output-ports"
    entity = api(session, "GET", f"/nifi-api/{endpoint}/{port_id}")
    status = entity["status"]["runStatus"]
    if status in {"Running", "Starting", "Invalid"}:
        return
    api(
        session,
        "PUT",
        f"/nifi-api/{endpoint}/{port_id}/run-status",
        json={
            "revision": entity["revision"],
            "state": "RUNNING",
            "disconnectedNodeAcknowledged": False,
        },
    )


def create_shared_services(session: requests.Session, platform_group_id: str) -> dict[str, str]:
    cache_server_id = create_controller_service(
        session,
        platform_group_id,
        "LogStormCacheServer",
        "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        {
            "Port": "4557",
            "Maximum Cache Entries": "10000",
            "Eviction Strategy": "Least Recently Used",
        },
    )
    cache_client_id = create_controller_service(
        session,
        platform_group_id,
        "LogStormCacheClient",
        "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
        {
            "Server Hostname": "localhost",
            "Server Port": "4557",
            "Communications Timeout": "30 secs",
        },
    )
    schema_registry_id = create_controller_service(
        session,
        platform_group_id,
        "AvroSchemaRegistry",
        "org.apache.nifi.schemaregistry.services.AvroSchemaRegistry",
        {"logstorm_event": LOGSTORM_AVRO_SCHEMA},
    )
    json_reader_id = create_controller_service(
        session,
        platform_group_id,
        "JsonTreeReader",
        "org.apache.nifi.json.JsonTreeReader",
        {
            "schema-access-strategy": "schema-name",
            "schema-registry": schema_registry_id,
            "schema-name": "logstorm_event",
        },
    )
    parquet_writer_id = create_controller_service(
        session,
        platform_group_id,
        "ParquetRecordSetWriter",
        "org.apache.nifi.parquet.ParquetRecordSetWriter",
        {
            "schema-access-strategy": "schema-name",
            "schema-registry": schema_registry_id,
            "schema-name": "logstorm_event",
        },
    )
    for service_id in [cache_server_id, cache_client_id, schema_registry_id, json_reader_id, parquet_writer_id]:
        enable_controller_service(session, service_id)
    return {
        "cache_client": cache_client_id,
        "json_reader": json_reader_id,
        "parquet_writer": parquet_writer_id,
    }


def build_sources_group(session: requests.Session, group_id: str) -> dict[str, str]:
    ports = {
        "raw_events": create_port(session, group_id, "raw_events", "output", (520.0, 120.0)),
        "source_failures": create_port(session, group_id, "source_failures", "output", (520.0, 260.0)),
    }
    listen = create_processor(
        session,
        group_id,
        "Listen Syslog",
        "org.apache.nifi.processors.standard.ListenSyslog",
        (120.0, 160.0),
        {"Protocol": "UDP", "Port": "5140", "Parse Messages": "false"},
    )
    update_processor(session, listen, {}, ["invalid"])
    create_connection(session, group_id, listen, ports["raw_events"], ["success"])
    return {"processors": [listen], "ports": ports}


def build_ingestion_group(session: requests.Session, group_id: str) -> dict[str, str]:
    ports = {
        "raw_events": create_port(session, group_id, "raw_events", "input", (20.0, 160.0)),
        "canonical_events": create_port(session, group_id, "canonical_events", "output", (760.0, 160.0)),
        "ingestion_failures": create_port(session, group_id, "ingestion_failures", "output", (760.0, 300.0)),
    }
    extract = create_processor(
        session,
        group_id,
        "Extract Syslog JSON",
        "org.apache.nifi.processors.standard.ExtractText",
        (180.0, 140.0),
        {"json_blob": "(\\{.*\\})$", "Enable DOTALL Mode": "true"},
    )
    prepare = create_processor(
        session,
        group_id,
        "Prepare Canonical JSON",
        "org.apache.nifi.processors.standard.ReplaceText",
        (440.0, 140.0),
        {
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Replacement Value": "${json_blob}",
        },
    )
    update_processor(session, extract, {}, ["failure", "unmatched"])
    update_processor(session, prepare, {}, ["failure"])
    create_connection(session, group_id, ports["raw_events"], extract, [])
    create_connection(session, group_id, extract, prepare, ["matched"])
    create_connection(session, group_id, prepare, ports["canonical_events"], ["success"])
    return {"processors": [extract, prepare], "ports": ports}


def build_quality_group(session: requests.Session, group_id: str, services: dict[str, str]) -> dict[str, str]:
    ports = {
        "canonical_events": create_port(session, group_id, "canonical_events", "input", (20.0, 160.0)),
        "validated_events": create_port(session, group_id, "validated_events", "output", (980.0, 120.0)),
        "invalid_events": create_port(session, group_id, "invalid_events", "output", (980.0, 260.0)),
        "duplicate_events": create_port(session, group_id, "duplicate_events", "output", (980.0, 400.0)),
        "contract_rejections": create_port(session, group_id, "contract_rejections", "output", (980.0, 520.0)),
    }
    contract_validate = create_processor(
        session,
        group_id,
        "Validate Contract (Python)",
        "org.apache.nifi.processors.standard.InvokeHTTP",
        (100.0, 140.0),
        {
            "HTTP Method": "POST",
            "Remote URL": DATAOPS_CONTRACT_URL,
            "Content-Type": "application/json",
            "Always Output Response": "true",
            "send-message-body": "true",
            "Use Chunked Encoding": "false",
            "Add Response Headers to Request": "false",
        },
    )
    route_contract = create_processor(
        session,
        group_id,
        "Route Contract Outcome",
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        (340.0, 140.0),
        {
            "valid_contract": "${invokehttp.status.code:equals('200')}",
            "invalid_contract": "${invokehttp.status.code:equals('422')}",
        },
    )
    evaluate = create_processor(
        session,
        group_id,
        "Extract Required Fields",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        (560.0, 140.0),
        {
            "Destination": "flowfile-attribute",
            "Return Type": "scalar",
            "Path Not Found Behavior": "ignore",
            "timestamp": "$.timestamp",
            "endpoint": "$.endpoint",
            "method": "$.method",
            "status_code": "$.status_code",
            "response_time_ms": "$.response_time_ms",
            "user_id": "$.user_id",
            "ip": "$.ip",
            "session_id": "$.session_id",
            "user_agent": "$.user_agent",
        },
    )
    validate = create_processor(
        session,
        group_id,
        "Validate Required Fields",
        "org.apache.nifi.processors.standard.RouteOnContent",
        (760.0, 140.0),
        {
            "valid_event": "(?s)^(?=.*\"timestamp\"\\s*:)(?=.*\"method\"\\s*:)(?=.*\"endpoint\"\\s*:)(?=.*\"status_code\"\\s*:)(?=.*\"response_time_ms\"\\s*:)(?=.*\"user_id\"\\s*:)(?=.*\"ip\"\\s*:).*$",
        },
    )
    dedupe = create_processor(
        session,
        group_id,
        "Detect Duplicate Events",
        "org.apache.nifi.processors.standard.DetectDuplicate",
        (920.0, 140.0),
        {
            "Cache Entry Identifier": "${user_id}_${timestamp}_${endpoint}",
            "Distributed Cache Service": services["cache_client"],
            "Age Off Duration": "30 min",
        },
    )
    update_processor(session, contract_validate, {}, ["Original", "Retry", "No Retry", "Failure"])
    update_processor(session, route_contract, {}, ["unmatched"])
    update_processor(session, evaluate, {}, ["failure", "unmatched"])
    update_processor(session, validate, {}, ["failure"])
    update_processor(session, dedupe, {}, ["failure"])
    create_connection(session, group_id, ports["canonical_events"], contract_validate, [])
    create_connection(session, group_id, contract_validate, route_contract, ["Response"])
    create_connection(session, group_id, route_contract, evaluate, ["valid_contract"])
    create_connection(session, group_id, route_contract, ports["contract_rejections"], ["invalid_contract"])
    create_connection(session, group_id, evaluate, validate, ["matched"])
    create_connection(session, group_id, validate, dedupe, ["valid_event"])
    create_connection(session, group_id, validate, ports["invalid_events"], ["unmatched"])
    create_connection(session, group_id, dedupe, ports["validated_events"], ["non-duplicate"])
    create_connection(session, group_id, dedupe, ports["duplicate_events"], ["duplicate"])
    return {"processors": [contract_validate, route_contract, evaluate, validate, dedupe], "ports": ports}


def build_transformation_group(session: requests.Session, group_id: str) -> dict[str, str]:
    ports = {
        "validated_events": create_port(session, group_id, "validated_events", "input", (20.0, 160.0)),
        "enriched_events": create_port(session, group_id, "enriched_events", "output", (1180.0, 120.0)),
        "enrichment_failures": create_port(session, group_id, "enrichment_failures", "output", (1180.0, 260.0)),
    }
    invoke_geoip = create_processor(
        session,
        group_id,
        "Fetch GeoIP",
        "org.apache.nifi.processors.standard.InvokeHTTP",
        (240.0, 140.0),
        {
            "HTTP Method": "POST",
            "Remote URL": GEOIP_URL,
            "Content-Type": "application/json",
            "Always Output Response": "true",
            "Put Response Body In Attribute": "invokehttp.response.body",
        },
    )
    enrich = create_processor(
        session,
        group_id,
        "Extract GeoIP Fields",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        (500.0, 140.0),
        {
            "Destination": "flowfile-attribute",
            "Return Type": "scalar",
            "Path Not Found Behavior": "ignore",
            "country": "$.country",
            "is_vpn": "$.is_vpn",
        },
    )
    stamp = create_processor(
        session,
        group_id,
        "Apply Enrichment Attributes",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        (760.0, 140.0),
        {
            "is_bot": "0",
            "pipeline.version": "1.0",
            "processed.at": "${now():format(\"yyyy-MM-dd'T'HH:mm:ssXXX\")}",
        },
    )
    build_json = create_processor(
        session,
        group_id,
        "Build Enriched Event JSON",
        "org.apache.nifi.processors.standard.AttributesToJSON",
        (1020.0, 140.0),
        {
            "Destination": "flowfile-content",
            "Include Core Attributes": "false",
            "Null Value": "false",
            "Attributes List": "timestamp,method,endpoint,status_code,response_time_ms,user_id,ip,session_id,user_agent,country,is_vpn,pipeline.version,processed.at,is_bot",
        },
    )
    update_processor(session, invoke_geoip, {}, ["Original", "Retry", "No Retry", "Failure"])
    update_processor(session, enrich, {}, ["failure", "unmatched"])
    update_processor(session, stamp, {}, [])
    update_processor(session, build_json, {}, ["failure"])
    create_connection(session, group_id, ports["validated_events"], invoke_geoip, [])
    create_connection(session, group_id, invoke_geoip, enrich, ["Response"])
    create_connection(session, group_id, enrich, stamp, ["matched"])
    create_connection(session, group_id, stamp, build_json, ["success"])
    create_connection(session, group_id, build_json, ports["enriched_events"], ["success"])
    return {"processors": [invoke_geoip, enrich, stamp, build_json], "ports": ports}


def build_routing_group(session: requests.Session, group_id: str) -> dict[str, str]:
    ports = {
        "enriched_events": create_port(session, group_id, "enriched_events", "input", (20.0, 160.0)),
        "normal_events": create_port(session, group_id, "normal_events", "output", (520.0, 80.0)),
        "slow_events": create_port(session, group_id, "slow_events", "output", (520.0, 200.0)),
        "error_events": create_port(session, group_id, "error_events", "output", (520.0, 320.0)),
    }
    route = create_processor(
        session,
        group_id,
        "Route Event Lanes",
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        (180.0, 140.0),
        {
            "error_lane": "${status_code:matches('4\\d\\d|5\\d\\d')}",
            "slow_lane": "${response_time_ms:toNumber():gt(1000)}",
            "normal_lane": "${status_code:toNumber():lt(400):and(${response_time_ms:toNumber():le(1000)})}",
        },
    )
    update_processor(session, route, {}, ["unmatched"])
    create_connection(session, group_id, ports["enriched_events"], route, [])
    create_connection(session, group_id, route, ports["normal_events"], ["normal_lane"])
    create_connection(session, group_id, route, ports["slow_events"], ["slow_lane"])
    create_connection(session, group_id, route, ports["error_events"], ["error_lane"])
    return {"processors": [route], "ports": ports}


def build_delivery_group(session: requests.Session, group_id: str, services: dict[str, str]) -> dict[str, str]:
    ports = {
        "events_for_storage": create_port(session, group_id, "events_for_storage", "input", (20.0, 160.0)),
        "storage_failures": create_port(session, group_id, "storage_failures", "output", (1040.0, 260.0)),
    }
    convert = create_processor(
        session,
        group_id,
        "Convert JSON To Parquet",
        "org.apache.nifi.processors.standard.ConvertRecord",
        (220.0, 140.0),
        {"record-reader": services["json_reader"], "record-writer": services["parquet_writer"]},
    )
    sanitize = create_processor(
        session,
        group_id,
        "Sanitize S3 Attributes",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        (500.0, 140.0),
        {"Delete Attributes Expression": "^(?!filename$).+"},
    )
    store = create_processor(
        session,
        group_id,
        "Write Raw Events Via Gateway",
        "org.apache.nifi.processors.standard.InvokeHTTP",
        (800.0, 140.0),
        {
            "HTTP Method": "PUT",
            "Remote URL": S3_GATEWAY_URL,
            "send-message-body": "true",
            "Use Chunked Encoding": "false",
            "Content-Type": "application/octet-stream",
            "Always Output Response": "false",
            "Add Response Headers to Request": "false",
            "x-logstorm-bucket": "logstorm-raw",
            "x-logstorm-key": "year=${now():format('yyyy')}/month=${now():format('MM')}/day=${now():format('dd')}/hour=${now():format('HH')}/${filename}",
        },
    )
    update_processor(session, convert, {}, ["failure"])
    update_processor(session, sanitize, {}, [])
    update_processor(session, store, {}, ["Original", "Response", "Failure", "No Retry", "Retry"])
    create_connection(session, group_id, ports["events_for_storage"], convert, [])
    create_connection(session, group_id, convert, sanitize, ["success"])
    create_connection(session, group_id, sanitize, store, ["success"])
    return {"processors": [convert, sanitize, store], "ports": ports}


def build_system_group(session: requests.Session, group_id: str) -> dict[str, str]:
    ports = {"error_events": create_port(session, group_id, "error_events", "input", (20.0, 160.0))}
    stamp = create_processor(
        session,
        group_id,
        "Set Error Filename",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        (180.0, 140.0),
        {"filename": "logstorm-error-${now():toNumber()}.json"},
    )
    archive = create_processor(
        session,
        group_id,
        "Archive Error Payload",
        "org.apache.nifi.processors.standard.PutFile",
        (440.0, 140.0),
        {"Directory": "/tmp", "Conflict Resolution Strategy": "replace"},
    )
    update_processor(session, stamp, {}, [])
    update_processor(session, archive, {}, ["failure", "success"])
    create_connection(session, group_id, ports["error_events"], stamp, [])
    create_connection(session, group_id, stamp, archive, ["success"])
    return {"processors": [stamp, archive], "ports": ports}


def start_all_processors(session: requests.Session, groups: list[dict[str, Any]]) -> None:
    for group in groups:
        for processor_id in group["processors"]:
            start_processor(session, processor_id)


def start_group_ports(session: requests.Session, groups: list[dict[str, Any]]) -> None:
    for group in groups:
        for port_id in group["ports"].values():
            component = resolve_component(session, port_id)
            port_kind = "input" if component["type"] == "INPUT_PORT" else "output"
            start_port(session, port_id, port_kind)


def main() -> None:
    session = requests.Session()
    wait_for_nifi(session)
    login(session)

    root_id = root_group_id(session)
    platform_id = get_or_create_process_group(session, root_id, PLATFORM_GROUP_NAME, (80.0, 80.0))

    child_groups = {
        name: get_or_create_process_group(session, platform_id, name, position)
        for name, position in GROUP_LAYOUT.items()
    }

    services = create_shared_services(session, platform_id)

    sources = build_sources_group(session, child_groups["01 Sources"])
    ingestion = build_ingestion_group(session, child_groups["02 Ingestion"])
    quality = build_quality_group(session, child_groups["03 Data Quality & Validation"], services)
    transform = build_transformation_group(session, child_groups["04 Transformation & Enrichment"])
    routing = build_routing_group(session, child_groups["05 Routing"])
    delivery = build_delivery_group(session, child_groups["06 Delivery & Storage"], services)
    system = build_system_group(session, child_groups["07 System & Errors"])

    create_connection(session, platform_id, sources["ports"]["raw_events"], ingestion["ports"]["raw_events"], [])
    create_connection(session, platform_id, sources["ports"]["source_failures"], system["ports"]["error_events"], [])
    create_connection(session, platform_id, ingestion["ports"]["canonical_events"], quality["ports"]["canonical_events"], [])
    create_connection(session, platform_id, ingestion["ports"]["ingestion_failures"], system["ports"]["error_events"], [])
    create_connection(session, platform_id, quality["ports"]["validated_events"], transform["ports"]["validated_events"], [])
    create_connection(session, platform_id, quality["ports"]["invalid_events"], system["ports"]["error_events"], [])
    create_connection(session, platform_id, quality["ports"]["duplicate_events"], system["ports"]["error_events"], [])
    create_connection(session, platform_id, quality["ports"]["contract_rejections"], system["ports"]["error_events"], [])
    create_connection(session, platform_id, transform["ports"]["enriched_events"], routing["ports"]["enriched_events"], [])
    create_connection(session, platform_id, transform["ports"]["enrichment_failures"], system["ports"]["error_events"], [])
    create_connection(session, platform_id, routing["ports"]["normal_events"], delivery["ports"]["events_for_storage"], [])
    create_connection(session, platform_id, routing["ports"]["slow_events"], delivery["ports"]["events_for_storage"], [])
    create_connection(session, platform_id, routing["ports"]["error_events"], delivery["ports"]["events_for_storage"], [])
    create_connection(session, platform_id, delivery["ports"]["storage_failures"], system["ports"]["error_events"], [])

    groups = [sources, ingestion, quality, transform, routing, delivery, system]
    start_group_ports(session, groups)
    start_all_processors(session, groups)
    print("NiFi layered bootstrap completed successfully")


if __name__ == "__main__":
    main()
