"""Fabric Ontology MCP Server — full CRUD for Ontology items in Microsoft Fabric."""

import json
import random
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional

from mcp.server.fastmcp import Context, FastMCP

from .definition_utils import decode_definition, encode_definition
from .fabric_client import FabricClient
from .kusto_client import KustoClient

_VALID_KUSTO_HOSTS = (".kusto.fabric.microsoft.com", ".kusto.windows.net")


@dataclass
class AppContext:
    client: FabricClient
    kusto: KustoClient


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    client = FabricClient()
    kusto = KustoClient()
    try:
        yield AppContext(client=client, kusto=kusto)
    finally:
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


def _validate_kusto_host(uri: str) -> None:
    """Ensure cluster URI points to a legitimate Kusto/Fabric endpoint."""
    from urllib.parse import urlparse
    host = urlparse(uri).hostname or ""
    if not any(host.endswith(suffix) for suffix in _VALID_KUSTO_HOSTS):
        raise ValueError(
            f"Invalid Kusto cluster URI. Host must end with one of: {', '.join(_VALID_KUSTO_HOSTS)}"
        )


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
    decoded = json.loads(definition_json)
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
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

    et_id = _generate_id()

    # Parse properties
    props_input = json.loads(properties) if isinstance(properties, str) else properties
    ts_props_input = json.loads(timeseries_properties) if isinstance(timeseries_properties, str) else timeseries_properties

    props = []
    first_prop_id = None
    for p in props_input:
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
    client = _client(ctx)
    decoded = await _get_decoded_definition(client, workspace_id, ontology_id)

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

    src_props = json.loads(source_table_properties) if isinstance(source_table_properties, str) else source_table_properties
    bindings = json.loads(property_bindings) if isinstance(property_bindings, str) else property_bindings

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


# ════════════════════════════════════════════════════════════════
#  OVERVIEW TOOLS
# ════════════════════════════════════════════════════════════════


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

    overview = json.loads(overview_json) if isinstance(overview_json, str) else overview_json
    et["overviews"] = overview
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return overview


# ════════════════════════════════════════════════════════════════
#  RESOURCE LINK TOOLS
# ════════════════════════════════════════════════════════════════


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

    links = json.loads(resource_links_json) if isinstance(resource_links_json, str) else resource_links_json
    et["resourceLinks"] = links
    await _push_definition(client, workspace_id, ontology_id, decoded)
    return links


# ════════════════════════════════════════════════════════════════
#  CONTEXTUALIZATION TOOLS
# ════════════════════════════════════════════════════════════════


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

    ctx_table = json.loads(data_binding_table) if isinstance(data_binding_table, str) else data_binding_table
    src_bindings = json.loads(source_key_ref_bindings) if isinstance(source_key_ref_bindings, str) else source_key_ref_bindings
    tgt_bindings = json.loads(target_key_ref_bindings) if isinstance(target_key_ref_bindings, str) else target_key_ref_bindings

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
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════


def main():
    mcp.run()


if __name__ == "__main__":
    main()
