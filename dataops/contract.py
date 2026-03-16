import ipaddress
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


CONTRACT_PATH = Path("contracts/logstorm-log-event.yml")


def load_contract(contract_path: Path | None = None) -> dict[str, Any]:
    path = contract_path or CONTRACT_PATH
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def contract_fields(contract: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return contract["models"]["logstorm_log_event"]["fields"]


def contract_schema_fields(contract: dict[str, Any]) -> list[dict[str, str]]:
    type_map = {
        "datetime": "datetime",
        "integer": "integer",
        "string": "string",
    }
    fields = []
    for name, spec in contract_fields(contract).items():
        fields.append({"name": name, "type": type_map.get(spec["type"], spec["type"])})
    return fields


def validate_record(record: dict[str, Any], contract: dict[str, Any] | None = None) -> list[dict[str, str]]:
    active_contract = contract or load_contract()
    violations: list[dict[str, str]] = []
    for field_name, spec in contract_fields(active_contract).items():
        value = record.get(field_name)
        if spec.get("required") and value in (None, ""):
            violations.append({"field": field_name, "reason": "required"})
            continue
        if value in (None, ""):
            continue
        if spec["type"] == "datetime":
            try:
                datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except ValueError:
                violations.append({"field": field_name, "reason": "invalid_datetime"})
        elif spec["type"] == "integer":
            try:
                int_value = int(float(value))
            except (TypeError, ValueError):
                violations.append({"field": field_name, "reason": "invalid_integer"})
                continue
            if "minimum" in spec and int_value < int(spec["minimum"]):
                violations.append({"field": field_name, "reason": "below_min"})
            if "maximum" in spec and int_value > int(spec["maximum"]):
                violations.append({"field": field_name, "reason": "above_max"})
        elif spec["type"] == "string":
            text = str(value)
            if "pattern" in spec and not re.fullmatch(spec["pattern"], text):
                violations.append({"field": field_name, "reason": "pattern_mismatch"})
            if spec.get("format") == "ipv4":
                try:
                    ipaddress.IPv4Address(text)
                except ipaddress.AddressValueError:
                    violations.append({"field": field_name, "reason": "invalid_ipv4"})
            if "enum" in spec and text not in spec["enum"]:
                violations.append({"field": field_name, "reason": "enum_mismatch"})
    return violations


def rejection_reason(violations: list[dict[str, str]]) -> str:
    return ";".join(f"{item['field']}:{item['reason']}" for item in violations) or "valid"


def contract_as_jsonschema(contract: dict[str, Any] | None = None) -> dict[str, Any]:
    active_contract = contract or load_contract()
    schema: dict[str, Any] = {"type": "object", "properties": {}, "required": []}
    for name, spec in contract_fields(active_contract).items():
        property_schema: dict[str, Any] = {}
        if spec["type"] == "datetime":
            property_schema = {"type": "string", "format": "date-time"}
        elif spec["type"] == "integer":
            property_schema = {"type": "integer"}
            if "minimum" in spec:
                property_schema["minimum"] = spec["minimum"]
            if "maximum" in spec:
                property_schema["maximum"] = spec["maximum"]
        elif spec["type"] == "string":
            property_schema = {"type": "string"}
            if "pattern" in spec:
                property_schema["pattern"] = spec["pattern"]
            if "enum" in spec:
                property_schema["enum"] = spec["enum"]
        schema["properties"][name] = property_schema
        if spec.get("required"):
            schema["required"].append(name)
    return schema


def dump_jsonschema(output_path: Path) -> None:
    output_path.write_text(json.dumps(contract_as_jsonschema(), indent=2), encoding="utf-8")
