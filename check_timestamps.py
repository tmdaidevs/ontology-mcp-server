"""Check datetime columns in relevant tables."""
import asyncio
import json
import subprocess
import httpx

CLUSTER = "https://trd-pxqduv45ps1nwvw094.z5.kusto.fabric.microsoft.com"
DB = "order_to_cash_eh"

def get_token():
    r = subprocess.run(
        [r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
         "account", "get-access-token", "--resource", CLUSTER, "-o", "json"],
        capture_output=True, text=True, timeout=30, shell=True, stdin=subprocess.DEVNULL
    )
    return json.loads(r.stdout)["accessToken"]

async def main():
    token = get_token()
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    tables = ["orders", "sla_metrics", "rework_metrics", "object_current_state",
              "variant_metrics", "plants", "customers"]
    
    async with httpx.AsyncClient(timeout=60) as c:
        for t in tables:
            kql = f'{t} | take 1 | getschema | where ColumnType == "datetime" | project ColumnName'
            body = {"db": DB, "csl": kql}
            resp = await c.post(f"{CLUSTER}/v1/rest/query", json=body, headers=h)
            data = resp.json()
            dt_cols = []
            for frame in data.get("Tables", []):
                for row in frame.get("Rows", []):
                    val = row[0]
                    if isinstance(val, str) and not val.startswith("20") and val != "0":
                        dt_cols.append(val)
            print(f"{t}: datetime columns = {dt_cols}")

asyncio.run(main())
