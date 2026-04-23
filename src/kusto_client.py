"""Async client for querying Kusto (Eventhouse) via the REST API."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .auth import get_access_token

logger = logging.getLogger(__name__)


class KustoClient:
    """Thin wrapper around the Kusto v1 REST query API."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=120)
        return self._client

    async def _headers(self, cluster_uri: str) -> dict[str, str]:
        token = await get_access_token(resource=cluster_uri)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def execute_query(
        self, cluster_uri: str, database: str, kql: str
    ) -> list[dict[str, Any]]:
        """Execute a KQL query and return parsed table frames.

        Each frame is a dict with 'Columns' (list of {ColumnName, DataType, ColumnType})
        and 'Rows' (list of lists).
        """
        client = await self._get_client()
        headers = await self._headers(cluster_uri)
        url = f"{cluster_uri.rstrip('/')}/v1/rest/query"

        resp = await client.post(
            url,
            json={"db": database, "csl": kql},
            headers=headers,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Kusto query failed ({resp.status_code}): {resp.text[:500]}"
            )

        data = resp.json()
        return data.get("Tables", [])

    async def list_tables(
        self, cluster_uri: str, database: str
    ) -> list[str]:
        """List all table names in a KQL database."""
        frames = await self.execute_query(
            cluster_uri, database, ".show tables | project TableName"
        )
        tables: list[str] = []
        for frame in frames:
            for row in frame.get("Rows", []):
                if row:
                    tables.append(str(row[0]))
        return tables

    async def get_table_schema(
        self, cluster_uri: str, database: str, table_name: str
    ) -> list[dict[str, str]]:
        """Get column schema for a specific table.

        Returns list of {'name': ..., 'type': ...} dicts.
        """
        frames = await self.execute_query(
            cluster_uri, database, f"{table_name} | getschema"
        )
        columns: list[dict[str, str]] = []
        for frame in frames:
            for row in frame.get("Rows", []):
                if len(row) >= 3:
                    columns.append({"name": str(row[0]), "type": str(row[2])})
                elif len(row) >= 2:
                    columns.append({"name": str(row[0]), "type": str(row[1])})
        return columns

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
