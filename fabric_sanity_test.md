```python
import os
import sys
import requests

# =========================
# CONFIG – set env vars OR paste values here
# =========================
TENANT_ID = os.getenv("FABRIC_TENANT_ID", "")
CLIENT_ID = os.getenv("FABRIC_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET", "")
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID", "")

def fail(msg: str, resp: requests.Response | None = None):
    print(f"❌ {msg}")
    if resp is not None:
        print(f"Status: {resp.status_code}")
        # Print response body, but keep it short
        text = resp.text
        print(text[:1500] + ("..." if len(text) > 1500 else ""))
    sys.exit(1)

# =========================
# AUTH: Get access token
# =========================
token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
token_payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "https://api.fabric.microsoft.com/.default",
}

token_response = requests.post(token_url, data=token_payload, timeout=30)
if token_response.status_code != 200:
    fail("Failed to get token (check tenant/client/secret).", token_response)

access_token = token_response.json().get("access_token")
if not access_token:
    fail("Token response did not include access_token.", token_response)

print("✅ Access token acquired")

# =========================
# CALL FABRIC API: list workspace items (metadata only)
# =========================
headers = {"Authorization": f"Bearer {access_token}"}
items_url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items"

items_response = requests.get(items_url, headers=headers, timeout=30)
if items_response.status_code != 200:
    fail("Fabric API call failed (likely permissions/workspace ID).", items_response)

payload = items_response.json()
items = payload.get("value", [])

print("✅ Fabric API call succeeded")
print(f"Found {len(items)} items:\n")

for item in items:
    print(f"- {item.get('type','?'):15} | {item.get('displayName','?')} | id={item.get('id','?')}")


# =========================
# RUN ON-DEMAND PIPELINE JOB
# =========================

PIPELINE_ID = ""

# Entra ID user with workspace access
OWNER_UPN = ""
OWNER_OBJECT_ID = ""


run_url = (
    f"https://api.fabric.microsoft.com/v1/"
    f"workspaces/{WORKSPACE_ID}/items/{PIPELINE_ID}/jobs/instances"
    f"?jobType=Pipeline"
)

headers_run = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

payload = {
    "executionData": {
        "pipelineName": "pipeline_import_excel_to_results",
        "OwnerUserPrincipalName": OWNER_UPN,
        "OwnerUserObjectId": OWNER_OBJECT_ID
    }
}

run_response = requests.post(
    run_url,
    headers=headers_run,
    json=payload,
    timeout=30
)

if run_response.status_code != 202:
    fail("Failed to start pipeline job.", run_response)

print("✅ Pipeline run accepted (202)")

# Job instance location (for polling)
job_location = run_response.headers.get("Location")
print("Job instance URL:", job_location)
```
