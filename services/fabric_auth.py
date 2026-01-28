"""
Microsoft Fabric / Entra ID OAuth client-credentials authentication.
Based on the proven pattern from fabric_sanity_test.md.
"""

import time
from dataclasses import dataclass

import requests

from config import get_fabric_config

SCOPE_FABRIC_API = "https://api.fabric.microsoft.com/.default"
SCOPE_ONELAKE_DFS = "https://storage.azure.com/.default"


@dataclass
class TokenInfo:
    access_token: str
    expires_at: float  # epoch seconds


# Separate caches per scope
_token_cache: dict[str, TokenInfo] = {}


def _acquire_token(scope: str) -> str:
    """
    Acquire an access token for the given scope via client_credentials grant.
    Caches per scope and refreshes when within 5 minutes of expiry.
    """
    cached = _token_cache.get(scope)
    if cached and time.time() < cached.expires_at - 300:
        return cached.access_token

    cfg = get_fabric_config()
    token_url = (
        f"https://login.microsoftonline.com/{cfg.tenant_id}/oauth2/v2.0/token"
    )
    payload = {
        "grant_type": "client_credentials",
        "client_id": cfg.client_id,
        "client_secret": cfg.client_secret,
        "scope": scope,
    }

    try:
        resp = requests.post(token_url, data=payload, timeout=30)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Token request failed (network error): {e}") from e

    if resp.status_code != 200:
        raise RuntimeError(
            f"Token acquisition failed ({resp.status_code}): {resp.text[:500]}"
        )

    try:
        body = resp.json()
    except ValueError as e:
        raise RuntimeError(
            f"Token response is not valid JSON: {e}"
        ) from e

    access_token = body.get("access_token")
    if not access_token:
        raise RuntimeError("Token response missing access_token field.")

    try:
        expires_in = int(body.get("expires_in", 3600))
    except (ValueError, TypeError):
        expires_in = 3600

    _token_cache[scope] = TokenInfo(
        access_token=access_token,
        expires_at=time.time() + expires_in,
    )
    return access_token


def get_access_token() -> str:
    """Acquire a token for the Fabric REST API."""
    return _acquire_token(SCOPE_FABRIC_API)


def get_storage_token() -> str:
    """Acquire a token for OneLake DFS (Azure Storage scope)."""
    return _acquire_token(SCOPE_ONELAKE_DFS)


def get_auth_headers() -> dict[str, str]:
    """Return Authorization headers for Fabric REST API calls."""
    return {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json",
    }


def get_storage_headers() -> dict[str, str]:
    """Return Authorization headers for OneLake DFS calls."""
    return {
        "Authorization": f"Bearer {get_storage_token()}",
    }
