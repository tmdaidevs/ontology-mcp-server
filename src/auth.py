"""Azure CLI token acquisition with per-resource caching.

Supports multiple audiences (Fabric API, Kusto clusters, etc.) with
independent cache entries keyed by resource URI.
"""

import asyncio
import json
import os
import platform
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime

_IS_WINDOWS = platform.system() == "Windows"

FABRIC_RESOURCE = "https://api.fabric.microsoft.com"


def _find_az() -> str:
    """Find the az CLI executable."""
    if _IS_WINDOWS:
        well_known = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
        if os.path.exists(well_known):
            return well_known
    found = shutil.which("az")
    if found:
        return found
    raise RuntimeError(
        "Azure CLI (az) not found. Install it from https://aka.ms/installazurecliwindows "
        "or ensure it is on your PATH."
    )


@dataclass
class _CachedToken:
    access_token: str
    expires_on: float


# Per-resource token cache
_cache: dict[str, _CachedToken] = {}


def _parse_expires_on(raw: str | int | float | None) -> float:
    """Parse expiresOn from Azure CLI — handles both epoch numbers and datetime strings."""
    if raw is None:
        return time.time() + 3600
    # Try numeric first
    try:
        return float(raw)
    except (ValueError, TypeError):
        pass
    # Try ISO-style datetime string (e.g. "2025-04-23 14:30:00.000000")
    if isinstance(raw, str):
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(raw, fmt)
                return dt.timestamp()
            except ValueError:
                continue
    return time.time() + 3600


def _get_token_sync(resource: str) -> str:
    """Synchronous token acquisition — runs az CLI subprocess."""
    cached = _cache.get(resource)
    if cached and cached.expires_on > time.time() + 60:
        return cached.access_token

    az = _find_az()
    if _IS_WINDOWS:
        cmd = f'"{az}" account get-access-token --resource {resource} -o json'
    else:
        cmd = [az, "account", "get-access-token", "--resource", resource, "-o", "json"]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        shell=_IS_WINDOWS,
        stdin=subprocess.DEVNULL,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Azure CLI authentication failed for resource '{resource}'. "
            f"Make sure you are logged in with 'az login'.\n"
            f"stderr: {result.stderr.strip()}"
        )

    try:
        token_data = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise RuntimeError("Azure CLI returned invalid JSON.")

    access_token = token_data.get("accessToken", "").strip()
    if not access_token:
        raise RuntimeError("Azure CLI returned empty access token.")

    expires_on = _parse_expires_on(token_data.get("expiresOn"))
    _cache[resource] = _CachedToken(access_token=access_token, expires_on=expires_on)
    return access_token


async def get_access_token(resource: str = FABRIC_RESOURCE) -> str:
    """Get a valid access token for the given resource using Azure CLI.

    Runs the blocking subprocess in a thread to avoid blocking the event loop.
    Tokens are cached per resource and reused until near expiry.
    """
    cached = _cache.get(resource)
    if cached and cached.expires_on > time.time() + 60:
        return cached.access_token
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_token_sync, resource)
