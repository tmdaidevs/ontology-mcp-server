"""Async client for OneLake Delta Table API (Unity Catalog compatible)."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from .auth import get_access_token

logger = logging.getLogger("fabric-ontology-mcp.onelake")

ONELAKE_TABLE_BASE = "https://onelake.table.fabric.microsoft.com/delta"
ONELAKE_TOKEN_AUDIENCE = "https://storage.azure.com/"

# Map Delta/Unity type_name to Ontology valueType
_TYPE_MAP = {
    "string": "String",
    "boolean": "Boolean",
    "date": "DateTime",
    "timestamp": "DateTime",
    "timestamp_ntz": "DateTime",
    "datetime": "DateTime",
    "int": "BigInt",
    "integer": "BigInt",
    "long": "BigInt",
    "bigint": "BigInt",
    "short": "BigInt",
    "tinyint": "BigInt",
    "smallint": "BigInt",
    "float": "Double",
    "double": "Double",
    "decimal": "Double",
}


def map_delta_type_to_ontology(type_name: str) -> str:
    """Map a Delta/Unity column type to an Ontology property valueType."""
    return _TYPE_MAP.get(type_name.lower(), "String")


class OneLakeClient:
    """Thin wrapper around the OneLake Delta Table API (Unity Catalog compatible)."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=120)
        return self._client

    async def _headers(self) -> dict[str, str]:
        token = await get_access_token(resource=ONELAKE_TOKEN_AUDIENCE)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def list_schemas(
        self, workspace_id: str, item_id: str
    ) -> list[dict[str, Any]]:
        """List schemas (namespaces) in a Lakehouse."""
        client = await self._get_client()
        headers = await self._headers()
        url = (
            f"{ONELAKE_TABLE_BASE}/{workspace_id}/{item_id}"
            f"/api/2.1/unity-catalog/schemas?catalog_name={item_id}"
        )
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(
                f"OneLake list_schemas failed ({resp.status_code}): {resp.text[:500]}"
            )
        return resp.json().get("schemas", [])

    async def list_tables(
        self, workspace_id: str, item_id: str, schema_name: str = "dbo"
    ) -> list[dict[str, Any]]:
        """List tables in a Lakehouse schema."""
        client = await self._get_client()
        headers = await self._headers()
        url = (
            f"{ONELAKE_TABLE_BASE}/{workspace_id}/{item_id}"
            f"/api/2.1/unity-catalog/tables"
            f"?catalog_name={item_id}&schema_name={schema_name}"
        )
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(
                f"OneLake list_tables failed ({resp.status_code}): {resp.text[:500]}"
            )
        return resp.json().get("tables", [])

    async def get_table(
        self,
        workspace_id: str,
        item_id: str,
        table_name: str,
        schema_name: str = "dbo",
    ) -> dict[str, Any]:
        """Get table definition including columns, types, and metadata."""
        client = await self._get_client()
        headers = await self._headers()
        full_table_name = f"{item_id}.{schema_name}.{table_name}"
        url = (
            f"{ONELAKE_TABLE_BASE}/{workspace_id}/{item_id}"
            f"/api/2.1/unity-catalog/tables/{full_table_name}"
        )
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(
                f"OneLake get_table failed ({resp.status_code}): {resp.text[:500]}"
            )
        return resp.json()

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
