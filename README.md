# Fabric Ontology MCP Server

Production-ready MCP server for full CRUD control of **Ontology** items in Microsoft Fabric.

## Prerequisites

- Python ≥ 3.11
- Azure CLI (`az`) installed and logged in (`az login`)
- Access to a Microsoft Fabric workspace with Ontology items

## Installation

```bash
cd "Ontology MCP Server"
pip install -e .
```

## Usage

```bash
# Run via entry point
fabric-ontology-mcp

# Or directly
python -m src
```

### VS Code / Copilot MCP config

```json
{
  "servers": {
    "fabric-ontology": {
      "command": "python",
      "args": ["-m", "src"],
      "cwd": "/path/to/Ontology MCP Server"
    }
  }
}
```

## Available Tools

### Workspace Discovery

| Tool | Description |
|------|-------------|
| `list_workspaces` | List all Fabric workspaces accessible to you |
| `list_workspace_items` | List items in a workspace (filter by type: Eventhouse, Lakehouse, etc.) |

### Ontology CRUD

| Tool | Description |
|------|-------------|
| `list_ontologies` | List ontologies in a workspace |
| `get_ontology` | Get ontology metadata |
| `create_ontology` | Create a new ontology |
| `update_ontology` | Update display name / description |
| `delete_ontology` | Delete an ontology (soft or hard) |
| `get_ontology_definition` | Get full decoded definition (entities, relationships, bindings) |
| `update_ontology_definition_raw` | Replace entire definition from JSON |

### Entity Types

| Tool | Description |
|------|-------------|
| `list_entity_types` | List all entity types in an ontology |
| `get_entity_type` | Get a single entity type with its bindings, documents, overviews, links |
| `add_entity_type` | Create a new entity type with properties |
| `update_entity_type` | Rename an entity type or change its display name property / ID parts |
| `remove_entity_type` | Delete an entity type (and its relationships) |

### Properties

| Tool | Description |
|------|-------------|
| `add_property` | Add a property to an entity type |
| `update_property` | Rename a property or change its value type |
| `remove_property` | Remove a property from an entity type |

### Relationship Types

| Tool | Description |
|------|-------------|
| `list_relationship_types` | List relationships in an ontology |
| `get_relationship_type` | Get a single relationship with its contextualizations |
| `add_relationship_type` | Create a relationship between entity types (validates both exist) |
| `update_relationship_type` | Rename a relationship type |
| `remove_relationship_type` | Delete a relationship type |

### Data Bindings

| Tool | Description |
|------|-------------|
| `list_data_bindings` | List bindings for an entity type |
| `add_data_binding` | Bind an entity type to a Lakehouse or Eventhouse table |
| `remove_data_binding` | Remove a data binding |

### Documents, Overviews & Resource Links

| Tool | Description |
|------|-------------|
| `add_document` | Attach a document URL to an entity type |
| `list_documents` | List all documents attached to an entity type |
| `remove_document` | Remove a document by URL |
| `get_overview` | Get the current overview configuration |
| `set_overview` | Configure overview widgets for an entity type |
| `get_resource_links` | Get the current resource links |
| `set_resource_links` | Link Power BI reports to an entity type |

### Contextualizations

| Tool | Description |
|------|-------------|
| `list_contextualizations` | List contextualizations for a relationship type |
| `add_contextualization` | Define how a relationship is materialized from data |
| `remove_contextualization` | Remove a contextualization |

### KQL / Eventhouse Discovery

| Tool | Description |
|------|-------------|
| `get_kql_database_details` | Get cluster URI, database name, and parent Eventhouse |
| `list_kql_tables` | List tables in a KQL database |
| `get_kql_table_schema` | Get column names and types for a table |

### Lakehouse & Workspace Data Discovery

| Tool | Description |
|------|-------------|
| `discover_lakehouse_tables` | List all tables in a Lakehouse (via OneLake Table API) |
| `get_lakehouse_table_schema` | Get columns with types and Ontology valueType mapping |
| `discover_workspace_data` | **Full scan** — discover all Lakehouses + Eventhouses, all tables and schemas |

> **Ontology planning workflow:** Call `discover_workspace_data` to scan a workspace, then use the returned schemas to design entity types, properties, relationships, and data bindings using the CRUD tools above.

## Project Structure

```
src/
├── __init__.py
├── __main__.py          # python -m src entry point
├── auth.py              # Azure CLI token acquisition (per-resource caching)
├── definition_utils.py  # Base64 encode/decode for ontology definition parts
├── fabric_client.py     # Async Fabric REST API client
├── kusto_client.py      # Async Kusto REST query client
└── server.py            # MCP server with all tools
```

## Input Validation

All tools validate inputs before calling the Fabric API:

- **Names** must match `^[a-zA-Z][a-zA-Z0-9_-]{0,127}$` (entity types, properties, relationships)
- **Value types** must be one of: `String`, `Boolean`, `DateTime`, `Object`, `BigInt`, `Double`
- **JSON parameters** return clear error messages on parse failure
- **Entity existence** is checked when creating relationships (both source and target must exist)

## Authentication

Uses Azure CLI tokens. Make sure you're logged in:

```bash
az login
```

Tokens are cached per resource (Fabric API and Kusto clusters are separate audiences) and automatically refreshed when near expiry.

## License

MIT
