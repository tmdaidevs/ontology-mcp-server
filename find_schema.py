"""Query Eventhouse tables schema via getSchema."""
import asyncio
import json
import subprocess
import httpx

CLUSTER_URI = "https://trd-pxqduv45ps1nwvw094.z5.kusto.fabric.microsoft.com"
DB_NAME = "order_to_cash_eh"

TABLES = ["orders", "object_current_state", "sla_metrics", "rework_metrics",
           "variant_metrics", "variant_catalog_csv", "customers", "plants"]

def get_kusto_token():
    result = subprocess.run(
        [r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
         "account", "get-access-token", "--resource", CLUSTER_URI, "-o", "json"],
        capture_output=True, text=True, timeout=30, shell=True, stdin=subprocess.DEVNULL
    )
    return json.loads(result.stdout)["accessToken"]

async def main():
    token = get_kusto_token()
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    query_url = f"{CLUSTER_URI}/v1/rest/query"

    async with httpx.AsyncClient(timeout=120) as c:
        for table in TABLES:
            body = {"db": DB_NAME, "csl": f"{table} | getschema"}
            resp = await c.post(query_url, json=body, headers=h)
            if resp.status_code == 200:
                data = resp.json()
                print(f"\n=== {table} ===")
                for frame in data.get("Tables", []):
                    rows = frame.get("Rows", [])
                    for row in rows:
                        if len(row) >= 3:
                            print(f"  {row[0]}: {row[2]}")
                        elif len(row) >= 2:
                            print(f"  {row[0]}: {row[1]}")
            else:
                print(f"\n=== {table} === ERROR {resp.status_code}")

asyncio.run(main())
