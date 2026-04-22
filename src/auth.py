"""Azure CLI token acquisition for Fabric API authentication."""

import asyncio
import json
import os
import platform
import shutil
import subprocess
import time
from dataclasses import dataclass
from functools import partial

FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
_IS_WINDOWS = platform.system() == "Windows"


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


_cached: _CachedToken | None = None


def _get_token_sync() -> str:
    """Synchronous token acquisition — runs az CLI subprocess."""
    global _cached
    if _cached and _cached.expires_on > time.time() + 60:
        return _cached.access_token

    az = _find_az()
    cmd = f'"{az}" account get-access-token --resource {FABRIC_RESOURCE} -o json' if _IS_WINDOWS else [az, "account", "get-access-token", "--resource", FABRIC_RESOURCE, "-o", "json"]

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
            f"Azure CLI authentication failed. Make sure you are logged in with 'az login'.\n"
            f"stderr: {result.stderr.strip()}"
        )

    try:
        token_data = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise RuntimeError("Azure CLI returned invalid JSON.")

    access_token = token_data.get("accessToken", "").strip()
    if not access_token:
        raise RuntimeError("Azure CLI returned empty access token.")

    try:
        expires_on = float(token_data.get("expiresOn", time.time() + 3600))
    except (ValueError, TypeError):
        expires_on = time.time() + 3600

    _cached = _CachedToken(access_token=access_token, expires_on=expires_on)
    return access_token


async def get_access_token() -> str:
    """Get a valid access token for the Fabric API using Azure CLI.

    Runs the blocking subprocess in a thread to avoid blocking the event loop.
    """
    if _cached and _cached.expires_on > time.time() + 60:
        return _cached.access_token
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_token_sync)
    return access_token
