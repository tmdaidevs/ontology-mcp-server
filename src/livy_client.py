"""Async client for Fabric Livy API — runs Spark SQL against Lakehouses."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .auth import get_access_token

logger = logging.getLogger("fabric-ontology-mcp.livy")

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
LIVY_API_VERSION = "2023-12-01"
SESSION_POLL_INTERVAL = 3  # seconds
SESSION_POLL_MAX = 40  # ~2 minutes max wait for session startup
STATEMENT_POLL_INTERVAL = 2
STATEMENT_POLL_MAX = 60  # ~2 minutes max for query


class LivyClient:
    """Run Spark SQL queries against a Lakehouse via the Fabric Livy API."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None
        # Cache active sessions: (workspace_id, lakehouse_id) -> session_id
        self._sessions: dict[tuple[str, str], int] = {}

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=120)
        return self._client

    async def _headers(self) -> dict[str, str]:
        token = await get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _livy_base(self, workspace_id: str, lakehouse_id: str) -> str:
        return (
            f"{FABRIC_BASE}/workspaces/{workspace_id}"
            f"/lakehouses/{lakehouse_id}"
            f"/livyApi/versions/{LIVY_API_VERSION}"
        )

    async def _get_or_create_session(
        self, workspace_id: str, lakehouse_id: str
    ) -> int:
        """Get an existing session or create a new one."""
        key = (workspace_id, lakehouse_id)

        # Check if we have a cached session that's still alive
        if key in self._sessions:
            session_id = self._sessions[key]
            try:
                client = await self._get_client()
                headers = await self._headers()
                url = f"{self._livy_base(workspace_id, lakehouse_id)}/sessions/{session_id}"
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    state = resp.json().get("state", "")
                    if state in ("idle", "busy"):
                        return session_id
            except Exception:
                pass
            del self._sessions[key]

        # Create a new session
        client = await self._get_client()
        headers = await self._headers()
        url = f"{self._livy_base(workspace_id, lakehouse_id)}/sessions"
        body = {"kind": "spark", "conf": {"spark.sql.shuffle.partitions": "4"}}

        logger.info("Creating Livy session for lakehouse %s...", lakehouse_id)
        resp = await client.post(url, json=body, headers=headers)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to create Livy session ({resp.status_code}): {resp.text[:500]}"
            )

        session_data = resp.json()
        session_id = session_data["id"]

        # Poll until session is ready
        for _ in range(SESSION_POLL_MAX):
            await asyncio.sleep(SESSION_POLL_INTERVAL)
            headers = await self._headers()
            resp = await client.get(
                f"{self._livy_base(workspace_id, lakehouse_id)}/sessions/{session_id}",
                headers=headers,
            )
            if resp.status_code == 200:
                state = resp.json().get("state", "")
                if state == "idle":
                    logger.info("Livy session %d ready", session_id)
                    self._sessions[key] = session_id
                    return session_id
                elif state in ("dead", "error", "killed"):
                    raise RuntimeError(f"Livy session failed with state: {state}")

        raise RuntimeError("Livy session startup timed out")

    async def execute_sql(
        self,
        workspace_id: str,
        lakehouse_id: str,
        sql: str,
    ) -> list[list[Any]]:
        """Execute a Spark SQL statement and return rows.

        Returns a list of rows, where each row is a list of values.
        """
        session_id = await self._get_or_create_session(workspace_id, lakehouse_id)
        client = await self._get_client()
        headers = await self._headers()

        url = (
            f"{self._livy_base(workspace_id, lakehouse_id)}"
            f"/sessions/{session_id}/statements"
        )
        body = {"code": sql, "kind": "sql"}

        resp = await client.post(url, json=body, headers=headers)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to submit statement ({resp.status_code}): {resp.text[:500]}"
            )

        stmt_data = resp.json()
        stmt_id = stmt_data["id"]

        # Poll for statement completion
        for _ in range(STATEMENT_POLL_MAX):
            await asyncio.sleep(STATEMENT_POLL_INTERVAL)
            headers = await self._headers()
            resp = await client.get(
                f"{url}/{stmt_id}",
                headers=headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                state = data.get("state", "")
                if state == "available":
                    output = data.get("output", {})
                    status = output.get("status", "")
                    if status == "ok":
                        result = output.get("data", {})
                        # Livy returns data in application/json format
                        if "application/json" in result:
                            json_data = result["application/json"]
                            return json_data.get("data", [])
                        # Or plain text
                        if "text/plain" in result:
                            return [{"result": result["text/plain"]}]
                        return []
                    else:
                        error_msg = output.get("evalue", "Unknown error")
                        raise RuntimeError(f"Spark SQL error: {error_msg}")
                elif state in ("error", "cancelled"):
                    raise RuntimeError(f"Statement failed with state: {state}")

        raise RuntimeError("Statement execution timed out")

    async def execute_sql_with_schema(
        self,
        workspace_id: str,
        lakehouse_id: str,
        sql: str,
    ) -> dict[str, Any]:
        """Execute SQL and return both schema and rows.

        Returns {"columns": [...], "rows": [[...], ...]}.
        """
        session_id = await self._get_or_create_session(workspace_id, lakehouse_id)
        client = await self._get_client()
        headers = await self._headers()

        url = (
            f"{self._livy_base(workspace_id, lakehouse_id)}"
            f"/sessions/{session_id}/statements"
        )
        body = {"code": sql, "kind": "sql"}

        resp = await client.post(url, json=body, headers=headers)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to submit statement ({resp.status_code}): {resp.text[:500]}"
            )

        stmt_data = resp.json()
        stmt_id = stmt_data["id"]

        for _ in range(STATEMENT_POLL_MAX):
            await asyncio.sleep(STATEMENT_POLL_INTERVAL)
            headers = await self._headers()
            resp = await client.get(f"{url}/{stmt_id}", headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                state = data.get("state", "")
                if state == "available":
                    output = data.get("output", {})
                    if output.get("status") == "ok":
                        result = output.get("data", {})
                        if "application/json" in result:
                            json_data = result["application/json"]
                            return {
                                "columns": json_data.get("schema", {}).get(
                                    "fields", []
                                ),
                                "rows": json_data.get("data", []),
                            }
                        return {"columns": [], "rows": []}
                    else:
                        raise RuntimeError(
                            f"Spark SQL error: {output.get('evalue', 'Unknown')}"
                        )
                elif state in ("error", "cancelled"):
                    raise RuntimeError(f"Statement failed: {state}")

        raise RuntimeError("Statement execution timed out")

    async def close(self) -> None:
        # Clean up sessions
        if self._client and not self._client.is_closed:
            for (ws_id, lh_id), session_id in self._sessions.items():
                try:
                    headers = await self._headers()
                    url = f"{self._livy_base(ws_id, lh_id)}/sessions/{session_id}"
                    await self._client.delete(url, headers=headers)
                except Exception:
                    pass
            await self._client.aclose()
        self._sessions.clear()
