"""Fabric Ontology MCP Server — full CRUD for Ontology items in Microsoft Fabric."""

import json
import logging
import random
import re
import sys
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional

from mcp.server.fastmcp import Context, FastMCP

from .definition_utils import decode_definition, encode_definition
from .fabric_client import FabricClient
from .kusto_client import KustoClient
from .livy_client import LivyClient
from .onelake_client import OneLakeClient, map_delta_type_to_ontology

# ── Logging (MCP requires stderr) ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("fabric-ontology-mcp")

_VALID_KUSTO_HOSTS = (".kusto.fabric.microsoft.com", ".kusto.windows.net")
_VALID_VALUE_TYPES = {"String", "Boolean", "DateTime", "Object", "BigInt", "Double"}
_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]{0,127}$")


@dataclass
class AppContext:
    client: FabricClient
    kusto: KustoClient
    onelake: OneLakeClient
    livy: LivyClient


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    client = FabricClient()
    kusto = KustoClient()
    onelake = OneLakeClient()
    livy = LivyClient()
    try:
        yield AppContext(client=client, kusto=kusto, onelake=onelake, livy=livy)
    finally:
        await livy.close()
        await onelake.close()
        await kusto.close()
        await client.close()


mcp = FastMCP(
    "Fabric Ontology MCP Server",
    instructions=(
        "This MCP server provides full control over Microsoft Fabric Ontology items. "
        "You can create, read, update, and delete ontologies, manage entity types with "
        "properties, define relationships, configure data bindings, documents, overviews, "
        "resource links, and contextualizations. "
        "Authentication uses Azure CLI — make sure you are logged in with 'az login'."
    ),
    lifespan=lifespan,
)


def _client(ctx: Context) -> FabricClient:
    return ctx.request_context.lifespan_context.client


def _kusto(ctx: Context) -> KustoClient:
    return ctx.request_context.lifespan_context.kusto


def _onelake(ctx: Context) -> OneLakeClient:
    return ctx.request_context.lifespan_context.onelake


def _livy(ctx: Context) -> LivyClient:
    return ctx.request_context.lifespan_context.livy


def _validate_kusto_host(uri: str) -> None:
    """Ensure cluster URI points to a legitimate Kusto/Fabric endpoint."""
    from urllib.parse import urlparse
    host = urlparse(uri).hostname or ""
    if not any(host.endswith(suffix) for suffix in _VALID_KUSTO_HOSTS):
        raise ValueError(
            f"Invalid Kusto cluster URI. Host must end with one of: {', '.join(_VALID_KUSTO_HOSTS)}"
        )


def _validate_name(value: str, field: str = "name") -> None:
    """Validate entity/relationship/property names against Fabric's naming rules."""
    if not _NAME_PATTERN.match(value):
        raise ValueError(
            f"Invalid {field}: '{value}'. Must start with a letter, contain only "
            f"letters/numbers/underscores/hyphens, and be 1–128 characters."
        )


def _validate_value_type(value_type: str) -> None:
    """Validate property value type against allowed values."""
    if value_type not in _VALID_VALUE_TYPES:
        raise ValueError(
            f"Invalid valueType: '{value_type}'. Must be one of: {', '.join(sorted(_VALID_VALUE_TYPES))}"
        )


def _parse_json(text: str, field: str) -> Any:
    """Parse a JSON string with a user-friendly error message."""
    if isinstance(text, (dict, list)):
        return text
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {field}: {e.msg} (position {e.pos})")


def _generate_id() -> str:
    """Generate a positive 64-bit integer ID as string."""
    return str(random.randint(10**12, 2**53))


# ════════════════════════════════════════════════════════════════
#  WORKSPACE TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def list_workspaces(ctx: Context) -> list[dict]:
    """List all Fabric workspaces accessible to the current user.

    Returns a list of workspaces with their IDs, names, and other metadata.
    Use this to find the workspace_id needed for other operations.
    """
    return await _client(ctx).list_workspaces()


@mcp.tool()
async def list_workspace_items(
    workspace_id: str,
    ctx: Context,
    item_type: Optional[str] = None,
) -> list[dict]:
    """List items in a Fabric workspace, optionally filtered by type.

    Useful for discovering Eventhouses, KQL Databases, Lakehouses, etc.

    Args:
        workspace_id: The workspace UUID.
        item_type: Optional type filter (e.g., Eventhouse, KQLDatabase, Lakehouse, Notebook, Pipeline, SemanticModel, Report).
    """
    return await _client(ctx).list_workspace_items(workspace_id, item_type)


# ════════════════════════════════════════════════════════════════
#  KQL DATABASE DISCOVERY TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def get_kql_database_details(
    workspace_id: str,
    kql_database_id: str,
    ctx: Context,
) -> dict:
    """Get details of a KQL database including its query URI, database name, and parent Eventhouse.

    Use this to obtain the cluster_uri and database_name needed for KQL queries.

    Args:
        workspace_id: The workspace UUID.
        kql_database_id: The KQL database item ID.
    """
    result = await _client(ctx).get_kql_database(workspace_id, kql_database_id)
    props = result.get("properties", {})
    return {
        "id": result.get("id"),
        "displayName": result.get("displayName"),
        "queryServiceUri": props.get("queryServiceUri"),
        "databaseName": props.get("databaseName"),
        "parentEventhouseItemId": props.get("parentEventhouseItemId"),
    }


@mcp.tool()
async def list_kql_tables(
    workspace_id: str,
    kql_database_id: str,
    ctx: Context,
) -> list[str]:
    """List all tables in a KQL database (Eventhouse).

    Resolves the cluster URI from the KQL database metadata — no raw URIs needed.

    Args:
        workspace_id: The workspace UUID.
        kql_database_id: The KQL database item ID.
    """
    db_info = await _client(ctx).get_kql_database(workspace_id, kql_database_id)
    props = db_info.get("properties", {})
    cluster_uri = props.get("queryServiceUri")
    db_name = props.get("databaseName")
    if not cluster_uri or not db_name:
        raise ValueError("Could not resolve cluster URI or database name from KQL database metadata.")
    _validate_kusto_host(cluster_uri)
    return await _kusto(ctx).list_tables(cluster_uri, db_name)


@mcp.tool()
async def get_kql_table_schema(
    workspace_id: str,
    kql_database_id: str,
    table_name: str,
    ctx: Context,
) -> list[dict]:
    """Get the column schema of a table in a KQL database.

    Returns each column's name and data type — useful for building data binding
    property mappings.

    Args:
        workspace_id: The workspace UUID.
        kql_database_id: The KQL database item ID.
        table_name: The table to inspect.
    """
    db_info = await _client(ctx).get_kql_database(workspace_id, kql_database_id)
    props = db_info.get("properties", {})
    cluster_uri = props.get("queryServiceUri")
    db_name = props.get("databaseName")
    if not cluster_uri or not db_name:
        raise ValueError("Could not resolve cluster URI or database name from KQL database metadata.")
    _validate_kusto_host(cluster_uri)
    return await _kusto(ctx).get_table_schema(cluster_uri, db_name, table_name)


# ════════════════════════════════════════════════════════════════
#  ONTOLOGY ITEM CRUD
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def list_ontologies(workspace_id: str, ctx: Context) -> list[dict]:
    """List all ontologies in a Fabric workspace.

    Args:
        workspace_id: The workspace UUID.
    """
    return await _client(ctx).list_ontologies(workspace_id)


@mcp.tool()
async def get_ontology(workspace_id: str, ontology_id: str, ctx: Context) -> dict:
    """Get metadata of a specific ontology.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
    """
    return await _client(ctx).get_ontology(workspace_id, ontology_id)


@mcp.tool()
async def create_ontology(
    workspace_id: str,
    display_name: str,
    ctx: Context,
    description: str = "",
) -> dict:
    """Create a new ontology in a workspace.

    Args:
        workspace_id: The workspace UUID.
        display_name: Name for the ontology. Must start with a letter, contain only letters/numbers/underscores, max 100 chars.
        description: Optional description (max 256 chars).
    """
    return await _client(ctx).create_ontology(workspace_id, display_name, description or None)


@mcp.tool()
async def update_ontology(
    workspace_id: str,
    ontology_id: str,
    ctx: Context,
    display_name: Optional[str] = None,
    description: Optional[str] = None,
) -> dict:
    """Update the display name and/or description of an ontology.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        display_name: New display name (optional).
        description: New description (optional).
    """
    return await _client(ctx).update_ontology(workspace_id, ontology_id, display_name, description)


@mcp.tool()
async def delete_ontology(
    workspace_id: str,
    ontology_id: str,
    ctx: Context,
    hard_delete: bool = False,
) -> str:
    """Delete an ontology.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        hard_delete: If True, permanently deletes the ontology (cannot be recovered).
    """
    await _client(ctx).delete_ontology(workspace_id, ontology_id, hard_delete)
    return f"Ontology {ontology_id} deleted successfully."


# ════════════════════════════════════════════════════════════════
#  ONTOLOGY DEFINITION — READ
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def get_ontology_definition(workspace_id: str, ontology_id: str, ctx: Context) -> dict:
    """Get the full ontology definition, decoded from Base64 into human-readable JSON.

    Returns entity types, relationship types, data bindings, documents, overviews,
    resource links, and contextualizations.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
    """
    raw = await _client(ctx).get_ontology_definition(workspace_id, ontology_id)
    parts = raw.get("definition", {}).get("parts", [])
    if not parts:
        return {"message": "Empty definition", "entityTypes": {}, "relationshipTypes": {}}
    return decode_definition(parts)


# ════════════════════════════════════════════════════════════════
#  ONTOLOGY DEFINITION — LOW-LEVEL UPDATE
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def update_ontology_definition_raw(
    workspace_id: str,
    ontology_id: str,
    definition_json: str,
    ctx: Context,
) -> str:
    """Update the full ontology definition with a raw JSON payload (advanced).

    The definition_json must be a JSON string representing the decoded definition
    structure (same format as returned by get_ontology_definition). It will be
    encoded to Base64 parts automatically.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        definition_json: JSON string of the full definition structure.
    """
    decoded = _parse_json(definition_json, "definition_json")
    parts = encode_definition(decoded)
    await _client(ctx).update_ontology_definition(
        workspace_id, ontology_id, {"parts": parts}
    )
    return "Ontology definition updated successfully."


# ════════════════════════════════════════════════════════════════
#  HELPER: read-modify-write pattern
# ════════════════════════════════════════════════════════════════


async def _get_decoded_definition(client: FabricClient, workspace_id: str, ontology_id: str) -> dict:
    raw = await client.get_ontology_definition(workspace_id, ontology_id)
    parts = raw.get("definition", {}).get("parts", [])
    if parts:
        return decode_definition(parts)
    return {
        "platform": {},
        "definition": {},
        "entityTypes": {},
        "relationshipTypes": {},
    }


async def _push_definition(client: FabricClient, workspace_id: str, ontology_id: str, decoded: dict) -> None:
    # Ensure platform metadata exists
    if not decoded.get("platform"):
        ontology = await client.get_ontology(workspace_id, ontology_id)
        decoded["platform"] = {
            "metadata": {
                "type": "Ontology",
                "displayName": ontology.get("displayName", "Ontology")
            }
        }
    parts = encode_definition(decoded)
    await client.update_ontology_definition(workspace_id, ontology_id, {"parts": parts})


# ════════════════════════════════════════════════════════════════
#  ENTITY TYPE TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def list_entity_types(workspace_id: str, ontology_id: str, ctx: Context) -> list[dict]:
    """List all entity types in an ontology.

    Returns a list of entity type definitions with their IDs, names, properties, etc.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
    """
    decoded = await _get_decoded_definition(_client(ctx), workspace_id, ontology_id)
    result = []
    for et_id, et_data in decoded.get("entityTypes", {}).items():
        defn = et_data.get("definition", {})
        if defn:
            result.append(defn)
    return result


@mcp.tool()
async def get_entity_type(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    ctx: Context,
) -> dict:
    """Get a single entity type by ID, including its data bindings, documents, overviews, and resource links.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
    """
    decoded = await _get_decoded_definition(_client(ctx), workspace_id, ontology_id)
    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")
    return et


@mcp.tool()
async def add_entity_type(
    workspace_id: str,
    ontology_id: str,
    name: str,
    ctx: Context,
    properties: str = "[]",
    timeseries_properties: str = "[]",
) -> dict:
    """Add a new entity type to an ontology.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        name: Entity type name. Must start with a letter, only letters/numbers/underscores/hyphens, max 128 chars.
        properties: JSON array of properties, each with "name" and "valueType" (String, Boolean, DateTime, Object, BigInt, Double). Example: [{"name": "DisplayName", "valueType": "String"}]
        timeseries_properties: JSON array of timeseries properties (same format as properties).
    """
    _validate_name(name, "entity type name")
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et_id = _generate_id()

    # Parse properties
    props_input = _parse_json(properties, "properties")
    ts_props_input = _parse_json(timeseries_properties, "timeseries_properties")

    props = []
    first_prop_id = None
    for p in props_input:
        _validate_name(p["name"], "property name")
        _validate_value_type(p.get("valueType", "String"))
        prop_id = _generate_id()
        if first_prop_id is None:
            first_prop_id = prop_id
        props.append({
            "id": prop_id,
            "name": p["name"],
            "valueType": p.get("valueType", "String"),
            "redefines": None,
            "baseTypeNamespaceType": None,
        })

    ts_props = []
    for p in ts_props_input:
        _validate_name(p["name"], "timeseries property name")
        _validate_value_type(p.get("valueType", "String"))
        ts_props.append({
            "id": _generate_id(),
            "name": p["name"],
            "valueType": p.get("valueType", "String"),
            "redefines": None,
            "baseTypeNamespaceType": None,
        })

    entity_def = {
        "id": et_id,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": name,
        "entityIdParts": [first_prop_id] if first_prop_id else [],
        "displayNamePropertyId": first_prop_id,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": props,
        "timeseriesProperties": ts_props,
    }

    decoded["entityTypes"][et_id] = {
        "definition": entity_def,
        "dataBindings": [],
        "documents": [],
        "overviews": {},
        "resourceLinks": {},
    }

    await _push_definition(client, workspace_id, ontology_id, decoded)
    return entity_def


@mcp.tool()
async def remove_entity_type(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    ctx: Context,
) -> str:
    """Remove an entity type from an ontology.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID to remove.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    if entity_type_id not in decoded.get("entityTypes", {}):
        return f"Entity type {entity_type_id} not found."

    del decoded["entityTypes"][entity_type_id]

    # Also remove any relationship types that reference this entity type
    to_remove = []
    for rt_id, rt_data in decoded.get("relationshipTypes", {}).items():
        rt_def = rt_data.get("definition", {})
        src = rt_def.get("source", {}).get("entityTypeId")
        tgt = rt_def.get("target", {}).get("entityTypeId")
        if src == entity_type_id or tgt == entity_type_id:
            to_remove.append(rt_id)
    for rt_id in to_remove:
        del decoded["relationshipTypes"][rt_id]

    await _push_definition(client, workspace_id, ontology_id, decoded)
    return f"Entity type {entity_type_id} removed successfully."


@mcp.tool()
async def update_entity_type(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    ctx: Context,
    name: Optional[str] = None,
    display_name_property_id: Optional[str] = None,
    entity_id_parts: Optional[str] = None,
) -> dict:
    """Update an entity type's name, display name property, or entity ID parts.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        name: New name for the entity type (optional).
        display_name_property_id: Property ID to use as display name (optional).
        entity_id_parts: JSON array of property IDs that uniquely identify entities (optional).
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    defn = et["definition"]
    if name is not None:
        _validate_name(name, "entity type name")
        defn["name"] = name
    if display_name_property_id is not None:
        defn["displayNamePropertyId"] = display_name_property_id
    if entity_id_parts is not None:
        defn["entityIdParts"] = _parse_json(entity_id_parts, "entity_id_parts")

    await _push_definition(client, workspace_id, ontology_id, decoded)
    logger.info("Updated entity type %s in ontology %s", entity_type_id, ontology_id)
    return defn


# ════════════════════════════════════════════════════════════════
#  PROPERTY TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def add_property(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    name: str,
    ctx: Context,
    value_type: str = "String",
    is_timeseries: bool = False,
) -> dict:
    """Add a property to an existing entity type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        name: Property name.
        value_type: One of: String, Boolean, DateTime, Object, BigInt, Double.
        is_timeseries: If True, adds as a timeseries property.
    """
    _validate_name(name, "property name")
    _validate_value_type(value_type)
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    prop = {
        "id": _generate_id(),
        "name": name,
        "valueType": value_type,
        "redefines": None,
        "baseTypeNamespaceType": None,
    }

    defn = et["definition"]
    if is_timeseries:
        defn.setdefault("timeseriesProperties", []).append(prop)
    else:
        defn.setdefault("properties", []).append(prop)

    await _push_definition(client, workspace_id, ontology_id, decoded)
    return prop


@mcp.tool()
async def remove_property(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    property_id: str,
    ctx: Context,
) -> str:
    """Remove a property from an entity type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        property_id: The property ID to remove.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    defn = et["definition"]
    defn["properties"] = [p for p in defn.get("properties", []) if str(p["id"]) != str(property_id)]
    defn["timeseriesProperties"] = [p for p in defn.get("timeseriesProperties", []) if str(p["id"]) != str(property_id)]

    await _push_definition(client, workspace_id, ontology_id, decoded)
    return f"Property {property_id} removed successfully."


@mcp.tool()
async def update_property(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    property_id: str,
    ctx: Context,
    name: Optional[str] = None,
    value_type: Optional[str] = None,
) -> dict:
    """Update a property's name or value type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        property_id: The property ID to update.
        name: New property name (optional).
        value_type: New value type (optional). One of: String, Boolean, DateTime, Object, BigInt, Double.
    """
    if name is not None:
        _validate_name(name, "property name")
    if value_type is not None:
        _validate_value_type(value_type)

    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    defn = et["definition"]
    target_prop = None
    for p in defn.get("properties", []) + defn.get("timeseriesProperties", []):
        if str(p["id"]) == str(property_id):
            target_prop = p
            break

    if not target_prop:
        raise ValueError(f"Property {property_id} not found in entity type {entity_type_id}.")

    if name is not None:
        target_prop["name"] = name
    if value_type is not None:
        target_prop["valueType"] = value_type

    await _push_definition(client, workspace_id, ontology_id, decoded)
    logger.info("Updated property %s in entity type %s", property_id, entity_type_id)
    return target_prop


# ════════════════════════════════════════════════════════════════
#  RELATIONSHIP TYPE TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def list_relationship_types(workspace_id: str, ontology_id: str, ctx: Context) -> list[dict]:
    """List all relationship types in an ontology.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
    """
    decoded = await _get_decoded_definition(_client(ctx), workspace_id, ontology_id)
    result = []
    for rt_id, rt_data in decoded.get("relationshipTypes", {}).items():
        defn = rt_data.get("definition", {})
        if defn:
            result.append(defn)
    return result


@mcp.tool()
async def get_relationship_type(
    workspace_id: str,
    ontology_id: str,
    relationship_type_id: str,
    ctx: Context,
) -> dict:
    """Get a single relationship type by ID, including its contextualizations.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        relationship_type_id: The relationship type ID.
    """
    decoded = await _get_decoded_definition(_client(ctx), workspace_id, ontology_id)
    rt = decoded.get("relationshipTypes", {}).get(relationship_type_id)
    if not rt:
        raise ValueError(f"Relationship type {relationship_type_id} not found.")
    return rt


@mcp.tool()
async def add_relationship_type(
    workspace_id: str,
    ontology_id: str,
    name: str,
    source_entity_type_id: str,
    target_entity_type_id: str,
    ctx: Context,
) -> dict:
    """Add a relationship type between two entity types.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        name: Relationship name (e.g., "contains", "belongsTo").
        source_entity_type_id: The source entity type ID.
        target_entity_type_id: The target entity type ID.
    """
    _validate_name(name, "relationship name")
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    # Validate entity types exist
    if source_entity_type_id not in decoded.get("entityTypes", {}):
        raise ValueError(f"Source entity type {source_entity_type_id} not found.")
    if target_entity_type_id not in decoded.get("entityTypes", {}):
        raise ValueError(f"Target entity type {target_entity_type_id} not found.")

    rt_id = _generate_id()
    rel_def = {
        "namespace": "usertypes",
        "id": rt_id,
        "name": name,
        "namespaceType": "Custom",
        "source": {"entityTypeId": source_entity_type_id},
        "target": {"entityTypeId": target_entity_type_id},
    }

    decoded["relationshipTypes"][rt_id] = {
        "definition": rel_def,
        "contextualizations": [],
    }

    await _push_definition(client, workspace_id, ontology_id, decoded)
    return rel_def


@mcp.tool()
async def remove_relationship_type(
    workspace_id: str,
    ontology_id: str,
    relationship_type_id: str,
    ctx: Context,
) -> str:
    """Remove a relationship type from an ontology.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        relationship_type_id: The relationship type ID to remove.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    if relationship_type_id not in decoded.get("relationshipTypes", {}):
        return f"Relationship type {relationship_type_id} not found."

    del decoded["relationshipTypes"][relationship_type_id]
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return f"Relationship type {relationship_type_id} removed successfully."


@mcp.tool()
async def update_relationship_type(
    workspace_id: str,
    ontology_id: str,
    relationship_type_id: str,
    name: str,
    ctx: Context,
) -> dict:
    """Rename a relationship type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        relationship_type_id: The relationship type ID.
        name: New name for the relationship type.
    """
    _validate_name(name, "relationship name")
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    rt = decoded.get("relationshipTypes", {}).get(relationship_type_id)
    if not rt:
        raise ValueError(f"Relationship type {relationship_type_id} not found.")

    rt["definition"]["name"] = name
    await _push_definition(client, workspace_id, ontology_id, decoded)
    logger.info("Updated relationship type %s", relationship_type_id)
    return rt["definition"]


# ════════════════════════════════════════════════════════════════
#  DATA BINDING TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def list_data_bindings(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    ctx: Context,
) -> list[dict]:
    """List all data bindings for an entity type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
    """
    decoded = await _get_decoded_definition(_client(ctx), workspace_id, ontology_id)
    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        return []
    return et.get("dataBindings", [])


@mcp.tool()
async def add_data_binding(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    binding_type: str,
    source_table_properties: str,
    property_bindings: str,
    ctx: Context,
    timestamp_column_name: Optional[str] = None,
) -> dict:
    """Add a data binding to an entity type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        binding_type: "TimeSeries" or "NonTimeSeries".
        source_table_properties: JSON string with source table config. For Lakehouse: {"sourceType": "LakehouseTable", "workspaceId": "...", "itemId": "...", "sourceTableName": "...", "sourceSchema": "dbo"}. For Eventhouse: {"sourceType": "KustoTable", "workspaceId": "...", "itemId": "...", "clusterUri": "...", "databaseName": "...", "sourceTableName": "..."}.
        property_bindings: JSON array of bindings: [{"sourceColumnName": "ColName", "targetPropertyId": "PropId"}, ...].
        timestamp_column_name: Required if binding_type is "TimeSeries". Name of the timestamp column.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    src_props = _parse_json(source_table_properties, "source_table_properties")
    bindings = _parse_json(property_bindings, "property_bindings")

    db_id = str(uuid.uuid4())
    data_binding = {
        "id": db_id,
        "dataBindingConfiguration": {
            "dataBindingType": binding_type,
            "propertyBindings": bindings,
            "sourceTableProperties": src_props,
        },
    }
    if timestamp_column_name:
        data_binding["dataBindingConfiguration"]["timestampColumnName"] = timestamp_column_name

    et.setdefault("dataBindings", []).append(data_binding)
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return data_binding


@mcp.tool()
async def remove_data_binding(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    data_binding_id: str,
    ctx: Context,
) -> str:
    """Remove a data binding from an entity type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        data_binding_id: The data binding UUID to remove.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    et["dataBindings"] = [db for db in et.get("dataBindings", []) if db.get("id") != data_binding_id]
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return f"Data binding {data_binding_id} removed successfully."


# ════════════════════════════════════════════════════════════════
#  DOCUMENT TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def add_document(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    url: str,
    ctx: Context,
    display_text: str = "",
) -> dict:
    """Add a document reference to an entity type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        url: URL pointing to the document.
        display_text: Display text for the document link.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    doc = {"displayText": display_text, "url": url}
    et.setdefault("documents", []).append(doc)
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return doc


@mcp.tool()
async def list_documents(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    ctx: Context,
) -> list[dict]:
    """List all documents attached to an entity type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
    """
    decoded = await _get_decoded_definition(_client(ctx), workspace_id, ontology_id)
    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")
    return et.get("documents", [])


@mcp.tool()
async def remove_document(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    document_url: str,
    ctx: Context,
) -> str:
    """Remove a document from an entity type by its URL.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        document_url: The URL of the document to remove.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    original_count = len(et.get("documents", []))
    et["documents"] = [d for d in et.get("documents", []) if d.get("url") != document_url]

    if len(et.get("documents", [])) == original_count:
        raise ValueError(f"Document with URL '{document_url}' not found.")

    await _push_definition(client, workspace_id, ontology_id, decoded)
    return f"Document removed successfully."


# ════════════════════════════════════════════════════════════════
#  OVERVIEW TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def get_overview(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    ctx: Context,
) -> dict:
    """Get the current overview configuration for an entity type.

    Returns widgets and settings, or an empty dict if no overview is configured.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
    """
    decoded = await _get_decoded_definition(_client(ctx), workspace_id, ontology_id)
    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")
    return et.get("overviews", {})


@mcp.tool()
async def set_overview(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    overview_json: str,
    ctx: Context,
) -> dict:
    """Set the overview configuration for an entity type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        overview_json: JSON string with overview config. Format: {"widgets": [{"id": "uuid", "type": "lineChart", "title": "...", "yAxisPropertyId": "..."}], "settings": {"type": "fixedTime", "fixedTimeRange": "Last1Hour", "interval": "OneHour", "aggregation": "Average"}}.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    overview = _parse_json(overview_json, "overview_json")
    et["overviews"] = overview
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return overview


# ════════════════════════════════════════════════════════════════
#  RESOURCE LINK TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def get_resource_links(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    ctx: Context,
) -> dict:
    """Get the current resource links for an entity type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
    """
    decoded = await _get_decoded_definition(_client(ctx), workspace_id, ontology_id)
    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")
    return et.get("resourceLinks", {})


@mcp.tool()
async def set_resource_links(
    workspace_id: str,
    ontology_id: str,
    entity_type_id: str,
    resource_links_json: str,
    ctx: Context,
) -> dict:
    """Set resource links for an entity type (e.g., Power BI reports).

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        entity_type_id: The entity type ID.
        resource_links_json: JSON string. Format: {"resourceLinks": [{"type": "PowerBIReport", "workspaceId": "...", "itemId": "..."}]}.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et = decoded.get("entityTypes", {}).get(entity_type_id)
    if not et:
        raise ValueError(f"Entity type {entity_type_id} not found.")

    links = _parse_json(resource_links_json, "resource_links_json")
    et["resourceLinks"] = links
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return links


# ════════════════════════════════════════════════════════════════
#  CONTEXTUALIZATION TOOLS
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def list_contextualizations(
    workspace_id: str,
    ontology_id: str,
    relationship_type_id: str,
    ctx: Context,
) -> list[dict]:
    """List all contextualizations for a relationship type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        relationship_type_id: The relationship type ID.
    """
    decoded = await _get_decoded_definition(_client(ctx), workspace_id, ontology_id)
    rt = decoded.get("relationshipTypes", {}).get(relationship_type_id)
    if not rt:
        raise ValueError(f"Relationship type {relationship_type_id} not found.")
    return rt.get("contextualizations", [])


@mcp.tool()
async def add_contextualization(
    workspace_id: str,
    ontology_id: str,
    relationship_type_id: str,
    data_binding_table: str,
    source_key_ref_bindings: str,
    target_key_ref_bindings: str,
    ctx: Context,
) -> dict:
    """Add a contextualization to a relationship type.

    Contextualizations define how relationships are materialized from data tables.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        relationship_type_id: The relationship type ID.
        data_binding_table: JSON string with lakehouse table config: {"sourceType": "LakehouseTable", "workspaceId": "...", "itemId": "...", "sourceTableName": "...", "sourceSchema": "dbo"}.
        source_key_ref_bindings: JSON array: [{"sourceColumnName": "Col", "targetPropertyId": "PropId"}].
        target_key_ref_bindings: JSON array: [{"sourceColumnName": "Col", "targetPropertyId": "PropId"}].
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    rt = decoded.get("relationshipTypes", {}).get(relationship_type_id)
    if not rt:
        raise ValueError(f"Relationship type {relationship_type_id} not found.")

    ctx_table = _parse_json(data_binding_table, "data_binding_table")
    src_bindings = _parse_json(source_key_ref_bindings, "source_key_ref_bindings")
    tgt_bindings = _parse_json(target_key_ref_bindings, "target_key_ref_bindings")

    ctx_id = str(uuid.uuid4())
    contextualization = {
        "id": ctx_id,
        "dataBindingTable": ctx_table,
        "sourceKeyRefBindings": src_bindings,
        "targetKeyRefBindings": tgt_bindings,
    }

    rt.setdefault("contextualizations", []).append(contextualization)
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return contextualization


@mcp.tool()
async def remove_contextualization(
    workspace_id: str,
    ontology_id: str,
    relationship_type_id: str,
    contextualization_id: str,
    ctx: Context,
) -> str:
    """Remove a contextualization from a relationship type.

    Args:
        workspace_id: The workspace UUID.
        ontology_id: The ontology UUID.
        relationship_type_id: The relationship type ID.
        contextualization_id: The contextualization UUID to remove.
    """
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    rt = decoded.get("relationshipTypes", {}).get(relationship_type_id)
    if not rt:
        raise ValueError(f"Relationship type {relationship_type_id} not found.")

    rt["contextualizations"] = [
        c for c in rt.get("contextualizations", []) if c.get("id") != contextualization_id
    ]
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return f"Contextualization {contextualization_id} removed successfully."


# ════════════════════════════════════════════════════════════════
#  LAKEHOUSE DATA DISCOVERY TOOLS (OneLake Table API)
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def discover_lakehouse_tables(
    workspace_id: str,
    lakehouse_id: str,
    ctx: Context,
) -> list[dict]:
    """List all tables in a Lakehouse with their names and formats.

    Uses the OneLake Delta Table API — no SQL endpoint needed.

    Args:
        workspace_id: The workspace UUID.
        lakehouse_id: The Lakehouse item ID.
    """
    tables = await _onelake(ctx).list_tables(workspace_id, lakehouse_id)
    return [
        {
            "name": t.get("name"),
            "schema": t.get("schema_name", "dbo"),
            "format": t.get("data_source_format"),
        }
        for t in tables
    ]


@mcp.tool()
async def get_lakehouse_table_schema(
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    ctx: Context,
    schema_name: str = "dbo",
) -> dict:
    """Get the full column schema of a Lakehouse table.

    Returns column names, types, nullability, and the mapped Ontology valueType
    for each column — useful for planning entity types and data bindings.

    Args:
        workspace_id: The workspace UUID.
        lakehouse_id: The Lakehouse item ID.
        table_name: The table name.
        schema_name: Schema name (default: "dbo").
    """
    table = await _onelake(ctx).get_table(
        workspace_id, lakehouse_id, table_name, schema_name
    )
    columns = []
    for col in table.get("columns", []):
        type_name = col.get("type_name", "string")
        columns.append({
            "name": col.get("name"),
            "type": type_name,
            "ontologyValueType": map_delta_type_to_ontology(type_name),
            "nullable": col.get("nullable", True),
            "position": col.get("position"),
        })
    return {
        "table": table_name,
        "schema": schema_name,
        "format": table.get("data_source_format"),
        "columns": columns,
    }


@mcp.tool()
async def discover_workspace_data(
    workspace_id: str,
    ctx: Context,
) -> dict:
    """Scan all Lakehouses and Eventhouses in a workspace and return their table schemas.

    This is the starting point for planning an ontology — it discovers all available
    data items and tables so you can design entity types, properties, and bindings.

    For each Lakehouse: uses the OneLake Delta Table API to get table schemas.
    For each Eventhouse/KQL Database: uses the Kusto REST API to get table schemas.

    Args:
        workspace_id: The workspace UUID.
    """
    fabric = _client(ctx)
    onelake = _onelake(ctx)
    kusto = _kusto(ctx)

    items = await fabric.list_workspace_items(workspace_id)

    result: dict[str, Any] = {
        "workspace_id": workspace_id,
        "lakehouses": [],
        "eventhouses": [],
    }

    # Discover Lakehouse tables
    lakehouses = [i for i in items if i.get("type") == "Lakehouse"]
    for lh in lakehouses:
        lh_id = lh["id"]
        lh_name = lh.get("displayName", lh_id)
        lh_entry: dict[str, Any] = {
            "id": lh_id,
            "name": lh_name,
            "tables": [],
        }
        try:
            tables = await onelake.list_tables(workspace_id, lh_id)
            for t in tables:
                t_name = t.get("name", "")
                try:
                    t_detail = await onelake.get_table(
                        workspace_id, lh_id, t_name
                    )
                    columns = [
                        {
                            "name": c.get("name"),
                            "type": c.get("type_name", "string"),
                            "ontologyValueType": map_delta_type_to_ontology(
                                c.get("type_name", "string")
                            ),
                        }
                        for c in t_detail.get("columns", [])
                    ]
                    lh_entry["tables"].append({
                        "name": t_name,
                        "columns": columns,
                    })
                except Exception as e:
                    logger.warning("Failed to get schema for %s.%s: %s", lh_name, t_name, e)
                    lh_entry["tables"].append({"name": t_name, "error": str(e)})
        except Exception as e:
            logger.warning("Failed to list tables for lakehouse %s: %s", lh_name, e)
            lh_entry["error"] = str(e)
        result["lakehouses"].append(lh_entry)

    # Discover Eventhouse/KQL Database tables
    kql_dbs = [i for i in items if i.get("type") == "KQLDatabase"]
    for db in kql_dbs:
        db_id = db["id"]
        db_name = db.get("displayName", db_id)
        eh_entry: dict[str, Any] = {
            "id": db_id,
            "name": db_name,
            "tables": [],
        }
        try:
            db_info = await fabric.get_kql_database(workspace_id, db_id)
            props = db_info.get("properties", {})
            cluster_uri = props.get("queryServiceUri")
            kql_db_name = props.get("databaseName")
            parent_eh = props.get("parentEventhouseItemId")
            eh_entry["clusterUri"] = cluster_uri
            eh_entry["databaseName"] = kql_db_name
            eh_entry["parentEventhouseId"] = parent_eh

            if cluster_uri and kql_db_name:
                _validate_kusto_host(cluster_uri)
                table_names = await kusto.list_tables(cluster_uri, kql_db_name)
                for t_name in table_names:
                    try:
                        cols = await kusto.get_table_schema(
                            cluster_uri, kql_db_name, t_name
                        )
                        eh_entry["tables"].append({
                            "name": t_name,
                            "columns": cols,
                        })
                    except Exception as e:
                        logger.warning("Failed to get schema for KQL %s.%s: %s", db_name, t_name, e)
                        eh_entry["tables"].append({"name": t_name, "error": str(e)})
        except Exception as e:
            logger.warning("Failed to discover KQL database %s: %s", db_name, e)
            eh_entry["error"] = str(e)
        result["eventhouses"].append(eh_entry)

    logger.info(
        "Discovered %d lakehouses, %d KQL databases in workspace %s",
        len(result["lakehouses"]),
        len(result["eventhouses"]),
        workspace_id,
    )
    return result


# ════════════════════════════════════════════════════════════════
#  DATA PROFILING TOOLS — KQL (Eventhouse)
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def preview_kql_table(
    workspace_id: str,
    kql_database_id: str,
    table_name: str,
    ctx: Context,
    limit: int = 100,
) -> dict:
    """Preview rows from a KQL table in an Eventhouse.

    Returns the first N rows to understand actual data values, patterns, and quality.

    Args:
        workspace_id: The workspace UUID.
        kql_database_id: The KQL database item ID.
        table_name: The table to preview.
        limit: Number of rows to return (default 100, max 1000).
    """
    limit = min(limit, 1000)
    db_info = await _client(ctx).get_kql_database(workspace_id, kql_database_id)
    props = db_info.get("properties", {})
    cluster_uri = props.get("queryServiceUri")
    db_name = props.get("databaseName")
    if not cluster_uri or not db_name:
        raise ValueError("Could not resolve cluster URI or database name.")
    _validate_kusto_host(cluster_uri)

    kql = f"{table_name} | take {limit}"
    frames = await _kusto(ctx).execute_query(cluster_uri, db_name, kql)

    rows = []
    columns = []
    for frame in frames:
        if frame.get("Columns"):
            columns = [
                {"name": c.get("ColumnName"), "type": c.get("DataType", c.get("ColumnType"))}
                for c in frame["Columns"]
            ]
        rows.extend(frame.get("Rows", []))

    return {"columns": columns, "rows": rows[:limit], "rowCount": len(rows)}


@mcp.tool()
async def profile_kql_table(
    workspace_id: str,
    kql_database_id: str,
    table_name: str,
    ctx: Context,
) -> dict:
    """Profile a KQL table — row count, distinct counts, null rates, sample values, and date ranges.

    Helps identify entity keys, foreign keys, optional properties, and timeseries columns.

    Args:
        workspace_id: The workspace UUID.
        kql_database_id: The KQL database item ID.
        table_name: The table to profile.
    """
    db_info = await _client(ctx).get_kql_database(workspace_id, kql_database_id)
    props = db_info.get("properties", {})
    cluster_uri = props.get("queryServiceUri")
    db_name = props.get("databaseName")
    if not cluster_uri or not db_name:
        raise ValueError("Could not resolve cluster URI or database name.")
    _validate_kusto_host(cluster_uri)

    kusto = _kusto(ctx)

    # Get row count
    count_frames = await kusto.execute_query(
        cluster_uri, db_name, f"{table_name} | count"
    )
    row_count = 0
    for f in count_frames:
        for r in f.get("Rows", []):
            if r:
                row_count = r[0]

    # Get schema
    schema = await kusto.get_table_schema(cluster_uri, db_name, table_name)

    # Profile each column
    col_profiles = []
    for col in schema:
        col_name = col["name"]
        col_type = col["type"]

        profile: dict[str, Any] = {
            "name": col_name,
            "type": col_type,
        }

        # Distinct count + null count + sample values in one query
        kql = (
            f"{table_name} | summarize "
            f"distinct_count=dcount({col_name}), "
            f"null_count=countif(isnull({col_name})), "
            f"total=count()"
        )
        try:
            frames = await kusto.execute_query(cluster_uri, db_name, kql)
            for f in frames:
                for r in f.get("Rows", []):
                    if len(r) >= 3:
                        profile["distinctCount"] = r[0]
                        profile["nullCount"] = r[1]
                        profile["nullRate"] = round(r[1] / max(r[2], 1), 4)
        except Exception:
            pass

        # Sample values (top 5 distinct)
        try:
            sample_kql = (
                f"{table_name} | where isnotnull({col_name}) "
                f"| summarize count() by {col_name} "
                f"| top 5 by count_ desc | project {col_name}"
            )
            frames = await kusto.execute_query(cluster_uri, db_name, sample_kql)
            samples = []
            for f in frames:
                for r in f.get("Rows", []):
                    if r:
                        samples.append(str(r[0]))
            profile["sampleValues"] = samples
        except Exception:
            pass

        # Min/max for date and numeric types
        if col_type in ("datetime", "int", "long", "real", "decimal", "double"):
            try:
                mm_kql = (
                    f"{table_name} | summarize "
                    f"min_val=min({col_name}), max_val=max({col_name})"
                )
                frames = await kusto.execute_query(cluster_uri, db_name, mm_kql)
                for f in frames:
                    for r in f.get("Rows", []):
                        if len(r) >= 2:
                            profile["min"] = str(r[0])
                            profile["max"] = str(r[1])
            except Exception:
                pass

        col_profiles.append(profile)

    return {
        "table": table_name,
        "rowCount": row_count,
        "columns": col_profiles,
    }


# ════════════════════════════════════════════════════════════════
#  DATA PROFILING TOOLS — Lakehouse (Spark SQL via Livy)
# ════════════════════════════════════════════════════════════════


@mcp.tool()
async def preview_lakehouse_table(
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    ctx: Context,
    limit: int = 100,
) -> dict:
    """Preview rows from a Lakehouse table using Spark SQL.

    First call may take 30-60 seconds for Spark session startup.
    Subsequent calls reuse the session and are fast.

    Args:
        workspace_id: The workspace UUID.
        lakehouse_id: The Lakehouse item ID.
        table_name: The table to preview.
        limit: Number of rows (default 100, max 1000).
    """
    limit = min(limit, 1000)
    sql = f"SELECT * FROM {table_name} LIMIT {limit}"
    result = await _livy(ctx).execute_sql_with_schema(
        workspace_id, lakehouse_id, sql
    )
    return {
        "columns": result.get("columns", []),
        "rows": result.get("rows", []),
        "rowCount": len(result.get("rows", [])),
    }


@mcp.tool()
async def profile_lakehouse_table(
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
    ctx: Context,
) -> dict:
    """Profile a Lakehouse table — row count, distinct counts, null rates, sample values.

    Runs Spark SQL to analyze data distributions. First call may take 30-60s for
    session startup, then queries are fast.

    Args:
        workspace_id: The workspace UUID.
        lakehouse_id: The Lakehouse item ID.
        table_name: The table to profile.
    """
    livy = _livy(ctx)

    # Get row count
    count_result = await livy.execute_sql(
        workspace_id, lakehouse_id,
        f"SELECT COUNT(*) as cnt FROM {table_name}"
    )
    row_count = count_result[0][0] if count_result else 0

    # Get column info
    schema_result = await livy.execute_sql_with_schema(
        workspace_id, lakehouse_id,
        f"SELECT * FROM {table_name} LIMIT 1"
    )
    columns = schema_result.get("columns", [])

    col_profiles = []
    for col_info in columns:
        col_name = col_info.get("name", "")
        col_type = col_info.get("type", "string")

        profile: dict[str, Any] = {
            "name": col_name,
            "type": col_type,
            "ontologyValueType": map_delta_type_to_ontology(col_type),
        }

        # Distinct count + null count
        try:
            stats_result = await livy.execute_sql(
                workspace_id, lakehouse_id,
                f"SELECT COUNT(DISTINCT `{col_name}`) as dc, "
                f"SUM(CASE WHEN `{col_name}` IS NULL THEN 1 ELSE 0 END) as nc, "
                f"COUNT(*) as total "
                f"FROM {table_name}"
            )
            if stats_result:
                row = stats_result[0]
                profile["distinctCount"] = row[0]
                profile["nullCount"] = row[1]
                profile["nullRate"] = round(row[1] / max(row[2], 1), 4)
        except Exception:
            pass

        # Sample values (top 5)
        try:
            sample_result = await livy.execute_sql(
                workspace_id, lakehouse_id,
                f"SELECT CAST(`{col_name}` AS STRING) as val, COUNT(*) as cnt "
                f"FROM {table_name} WHERE `{col_name}` IS NOT NULL "
                f"GROUP BY `{col_name}` ORDER BY cnt DESC LIMIT 5"
            )
            profile["sampleValues"] = [str(r[0]) for r in sample_result if r]
        except Exception:
            pass

        # Min/max for date/numeric types
        if col_type in ("date", "timestamp", "timestamp_ntz", "datetime",
                        "int", "integer", "long", "bigint", "short",
                        "float", "double", "decimal"):
            try:
                mm_result = await livy.execute_sql(
                    workspace_id, lakehouse_id,
                    f"SELECT CAST(MIN(`{col_name}`) AS STRING), "
                    f"CAST(MAX(`{col_name}`) AS STRING) FROM {table_name}"
                )
                if mm_result:
                    profile["min"] = str(mm_result[0][0])
                    profile["max"] = str(mm_result[0][1])
            except Exception:
                pass

        col_profiles.append(profile)

    return {
        "table": table_name,
        "rowCount": row_count,
        "columns": col_profiles,
    }


# ════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════


def main():
    mcp.run()


if __name__ == "__main__":
    main()
