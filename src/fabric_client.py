"""Async HTTP client for the Fabric Ontology REST API."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .auth import get_access_token

# Suppress httpx INFO logging which interferes with stdio MCP transport
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

BASE_URL = "https://api.fabric.microsoft.com/v1"
MAX_LRO_POLLS = 60
LRO_POLL_INTERVAL = 2  # seconds


class FabricClient:
    """Wraps the Fabric Ontology REST API with automatic auth, retry, and LRO polling."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=60)
        return self._client

    async def _headers(self) -> dict[str, str]:
        token = await get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
        params: dict | None = None,
    ) -> dict | list | None:
        client = await self._get_client()
        url = f"{BASE_URL}{path}"

        for attempt in range(3):
            headers = await self._headers()
            resp = await client.request(
                method,
                url,
                headers=headers,
                json=json_body,
                params=params,
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "5"))
                await asyncio.sleep(retry_after)
                continue

            if resp.status_code == 202:
                return await self._poll_lro(resp)

            if resp.status_code in (200, 201):
                if resp.content:
                    return resp.json()
                return None

            # Error
            try:
                error = resp.json()
            except Exception:
                error = {"message": resp.text}
            raise RuntimeError(
                f"Fabric API error {resp.status_code}: {error}"
            )

        raise RuntimeError("Max retries exceeded for Fabric API request")

    async def _poll_lro(self, initial_response: httpx.Response) -> dict | None:
        """Poll a long-running operation until completion."""
        location = initial_response.headers.get("Location")
        operation_id = initial_response.headers.get("x-ms-operation-id")

        if not location and not operation_id:
            return None

        poll_url = location or f"{BASE_URL}/operations/{operation_id}"
        client = await self._get_client()

        for _ in range(MAX_LRO_POLLS):
            await asyncio.sleep(LRO_POLL_INTERVAL)
            headers = await self._headers()
            resp = await client.get(poll_url, headers=headers)

            if resp.status_code == 200:
                data = resp.json()
                status = data.get("status", "").lower()
                if status in ("succeeded", "completed"):
                    # Try /operations/{id}/result first (Fabric pattern)
                    if operation_id:
                        result_url = f"{BASE_URL}/operations/{operation_id}/result"
                        result_resp = await client.get(result_url, headers=await self._headers())
                        if result_resp.status_code == 200 and result_resp.content:
                            return result_resp.json()
                    # Fallback to resourceLocation
                    result_url = data.get("resourceLocation")
                    if result_url:
                        result_resp = await client.get(result_url, headers=await self._headers())
                        if result_resp.status_code == 200 and result_resp.content:
                            return result_resp.json()
                    return data
                elif status in ("failed", "cancelled"):
                    raise RuntimeError(f"LRO failed: {data}")
            elif resp.status_code == 202:
                continue
            else:
                raise RuntimeError(f"LRO poll error {resp.status_code}: {resp.text}")

        raise RuntimeError("LRO polling timed out")

    # ── Ontology Item CRUD ──

    async def list_ontologies(self, workspace_id: str) -> list[dict]:
        """List all ontologies in a workspace."""
        result = await self._request("GET", f"/workspaces/{workspace_id}/ontologies")
        if isinstance(result, dict):
            return result.get("value", [])
        return result or []

    async def get_ontology(self, workspace_id: str, ontology_id: str) -> dict:
        """Get ontology metadata."""
        result = await self._request("GET", f"/workspaces/{workspace_id}/ontologies/{ontology_id}")
        return result or {}

    async def create_ontology(
        self,
        workspace_id: str,
        display_name: str,
        description: str | None = None,
        definition: dict | None = None,
    ) -> dict:
        """Create a new ontology."""
        body: dict[str, Any] = {"displayName": display_name}
        if description:
            body["description"] = description
        if definition:
            body["definition"] = definition
        result = await self._request("POST", f"/workspaces/{workspace_id}/ontologies", json_body=body)
        return result or {}

    async def update_ontology(
        self,
        workspace_id: str,
        ontology_id: str,
        display_name: str | None = None,
        description: str | None = None,
    ) -> dict:
        """Update ontology display name and/or description."""
        body: dict[str, Any] = {}
        if display_name is not None:
            body["displayName"] = display_name
        if description is not None:
            body["description"] = description
        result = await self._request(
            "PATCH", f"/workspaces/{workspace_id}/ontologies/{ontology_id}", json_body=body
        )
        return result or {}

    async def delete_ontology(
        self, workspace_id: str, ontology_id: str, hard_delete: bool = False
    ) -> None:
        """Delete an ontology."""
        params = {}
        if hard_delete:
            params["hardDelete"] = "True"
        await self._request(
            "DELETE", f"/workspaces/{workspace_id}/ontologies/{ontology_id}", params=params
        )

    # ── Ontology Definition ──

    async def get_ontology_definition(
        self, workspace_id: str, ontology_id: str
    ) -> dict:
        """Get the ontology definition (raw parts)."""
        result = await self._request(
            "POST",
            f"/workspaces/{workspace_id}/ontologies/{ontology_id}/getDefinition",
        )
        return result or {}

    async def update_ontology_definition(
        self,
        workspace_id: str,
        ontology_id: str,
        definition: dict,
        update_metadata: bool = True,
    ) -> None:
        """Update the ontology definition (replaces entire definition)."""
        params = {}
        if update_metadata:
            params["updateMetadata"] = "True"
        await self._request(
            "POST",
            f"/workspaces/{workspace_id}/ontologies/{ontology_id}/updateDefinition",
            json_body={"definition": definition},
            params=params,
        )

    # ── Workspace listing ──

    async def list_workspaces(self) -> list[dict]:
        """List all workspaces accessible to the current user."""
        result = await self._request("GET", "/workspaces")
        if isinstance(result, dict):
            return result.get("value", [])
        return result or []

    async def list_workspace_items(
        self, workspace_id: str, item_type: str | None = None
    ) -> list[dict]:
        """List items in a workspace, optionally filtered by type."""
        params = {}
        if item_type:
            params["type"] = item_type
        result = await self._request(
            "GET", f"/workspaces/{workspace_id}/items", params=params or None
        )
        if isinstance(result, dict):
            return result.get("value", [])
        return result or []

    # ── KQL Database details ──

    async def get_kql_database(
        self, workspace_id: str, kql_database_id: str
    ) -> dict:
        """Get KQL database details including queryServiceUri and databaseName."""
        result = await self._request(
            "GET", f"/workspaces/{workspace_id}/kqlDatabases/{kql_database_id}"
        )
        return result or {}

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
