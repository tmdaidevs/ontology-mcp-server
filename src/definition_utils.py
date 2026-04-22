"""Utilities for encoding/decoding Fabric Ontology definition parts (Base64 JSON)."""

from __future__ import annotations

import base64
import json
import re
from typing import Any


def b64_decode(payload: str) -> Any:
    """Decode a Base64 payload to a Python object (JSON)."""
    raw = base64.b64decode(payload).decode("utf-8")
    if not raw.strip():
        return {}
    return json.loads(raw)


def b64_encode(obj: Any) -> str:
    """Encode a Python object as Base64 JSON string."""
    raw = json.dumps(obj, ensure_ascii=False)
    return base64.b64encode(raw.encode("utf-8")).decode("ascii")


def decode_definition(parts: list[dict]) -> dict[str, Any]:
    """Decode all parts of an ontology definition into a structured dict.

    Returns:
        {
            "platform": {...},
            "definition": {...},
            "entityTypes": { "<id>": { "definition": {...}, "dataBindings": [...], "documents": [...], "overviews": {...}, "resourceLinks": {...} } },
            "relationshipTypes": { "<id>": { "definition": {...}, "contextualizations": [...] } }
        }
    """
    result: dict[str, Any] = {
        "platform": {},
        "definition": {},
        "entityTypes": {},
        "relationshipTypes": {},
    }

    for part in parts:
        path: str = part["path"]
        payload = part.get("payload", "")
        decoded = b64_decode(payload) if payload else {}

        if path == ".platform":
            result["platform"] = decoded
        elif path == "definition.json":
            result["definition"] = decoded
        elif path.startswith("EntityTypes/"):
            _parse_entity_type_part(result, path, decoded)
        elif path.startswith("RelationshipTypes/"):
            _parse_relationship_type_part(result, path, decoded)

    return result


def _parse_entity_type_part(result: dict, path: str, decoded: Any) -> None:
    # EntityTypes/{ID}/definition.json
    m = re.match(r"EntityTypes/([^/]+)/definition\.json$", path)
    if m:
        et_id = m.group(1)
        result["entityTypes"].setdefault(et_id, _empty_entity())
        result["entityTypes"][et_id]["definition"] = decoded
        return

    # EntityTypes/{ID}/DataBindings/{GUID}.json
    m = re.match(r"EntityTypes/([^/]+)/DataBindings/([^/]+)\.json$", path)
    if m:
        et_id = m.group(1)
        result["entityTypes"].setdefault(et_id, _empty_entity())
        result["entityTypes"][et_id]["dataBindings"].append(decoded)
        return

    # EntityTypes/{ID}/Documents/{name}.json
    m = re.match(r"EntityTypes/([^/]+)/Documents/([^/]+)\.json$", path)
    if m:
        et_id = m.group(1)
        result["entityTypes"].setdefault(et_id, _empty_entity())
        result["entityTypes"][et_id]["documents"].append(decoded)
        return

    # EntityTypes/{ID}/Overviews/definition.json
    m = re.match(r"EntityTypes/([^/]+)/Overviews/definition\.json$", path)
    if m:
        et_id = m.group(1)
        result["entityTypes"].setdefault(et_id, _empty_entity())
        result["entityTypes"][et_id]["overviews"] = decoded
        return

    # EntityTypes/{ID}/ResourceLinks/definition.json
    m = re.match(r"EntityTypes/([^/]+)/ResourceLinks/definition\.json$", path)
    if m:
        et_id = m.group(1)
        result["entityTypes"].setdefault(et_id, _empty_entity())
        result["entityTypes"][et_id]["resourceLinks"] = decoded
        return


def _parse_relationship_type_part(result: dict, path: str, decoded: Any) -> None:
    # RelationshipTypes/{ID}/definition.json
    m = re.match(r"RelationshipTypes/([^/]+)/definition\.json$", path)
    if m:
        rt_id = m.group(1)
        result["relationshipTypes"].setdefault(rt_id, _empty_relationship())
        result["relationshipTypes"][rt_id]["definition"] = decoded
        return

    # RelationshipTypes/{ID}/Contextualizations/{GUID}.json
    m = re.match(r"RelationshipTypes/([^/]+)/Contextualizations/([^/]+)\.json$", path)
    if m:
        rt_id = m.group(1)
        result["relationshipTypes"].setdefault(rt_id, _empty_relationship())
        result["relationshipTypes"][rt_id]["contextualizations"].append(decoded)
        return


def _empty_entity() -> dict:
    return {"definition": {}, "dataBindings": [], "documents": [], "overviews": {}, "resourceLinks": {}}


def _empty_relationship() -> dict:
    return {"definition": {}, "contextualizations": []}


def encode_definition(decoded: dict[str, Any]) -> list[dict]:
    """Encode a structured definition dict back into Base64 parts.

    Accepts the same structure returned by decode_definition().
    """
    parts: list[dict] = []

    # .platform
    if decoded.get("platform"):
        parts.append(_make_part(".platform", decoded["platform"]))

    # definition.json
    parts.append(_make_part("definition.json", decoded.get("definition", {})))

    # Entity types
    for et_id, et_data in decoded.get("entityTypes", {}).items():
        if et_data.get("definition"):
            parts.append(_make_part(f"EntityTypes/{et_id}/definition.json", et_data["definition"]))

        for db in et_data.get("dataBindings", []):
            db_id = db.get("id", "unknown")
            parts.append(_make_part(f"EntityTypes/{et_id}/DataBindings/{db_id}.json", db))

        for i, doc in enumerate(et_data.get("documents", []), 1):
            parts.append(_make_part(f"EntityTypes/{et_id}/Documents/document{i}.json", doc))

        if et_data.get("overviews"):
            parts.append(_make_part(f"EntityTypes/{et_id}/Overviews/definition.json", et_data["overviews"]))

        if et_data.get("resourceLinks"):
            parts.append(_make_part(f"EntityTypes/{et_id}/ResourceLinks/definition.json", et_data["resourceLinks"]))

    # Relationship types
    for rt_id, rt_data in decoded.get("relationshipTypes", {}).items():
        if rt_data.get("definition"):
            parts.append(_make_part(f"RelationshipTypes/{rt_id}/definition.json", rt_data["definition"]))

        for ctx in rt_data.get("contextualizations", []):
            ctx_id = ctx.get("id", "unknown")
            parts.append(_make_part(f"RelationshipTypes/{rt_id}/Contextualizations/{ctx_id}.json", ctx))

    return parts


def _make_part(path: str, payload_obj: Any) -> dict:
    return {
        "path": path,
        "payload": b64_encode(payload_obj),
        "payloadType": "InlineBase64",
    }
