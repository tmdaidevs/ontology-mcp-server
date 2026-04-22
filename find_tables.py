"""Query Eventhouse tables and columns for data binding mapping."""
import asyncio
import json
import subprocess
import httpx

CLUSTER_URI = "https://trd-pxqduv45ps1nwvw094.z5.kusto.fabric.microsoft.com"
DB_NAME = "order_to_cash_eh"

def get_kusto_token():
    result = subprocess.run(
        [r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
         "account", "get-access-token",
         "--resource", CLUSTER_URI,
         "-o", "json"],
        capture_output=True, text=True, timeout=30,
        shell=True, stdin=subprocess.DEVNULL
    )
    return json.loads(result.stdout)["accessToken"]

async def main():
    token = get_kusto_token()
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=120) as c:
        # Query tables
        query_url = f"{CLUSTER_URI}/v1/rest/query"
        body = {
            "db": DB_NAME,
            "csl": ".show tables | project TableName"
        }
        resp = await c.post(query_url, json=body, headers=h)
        if resp.status_code == 200:
            data = resp.json()
            tables = []
            for frame in data.get("Tables", []):
                for row in frame.get("Rows", []):
                    tables.append(row[0])
            print(f"Tables in {DB_NAME}: {len(tables)}")
            for t in tables:
                print(f"  - {t}")

            # Get columns for each relevant table
            relevant = ["orders", "object_current_state", "sla_metrics", "rework_metrics",
                        "variant_metrics", "variant_catalog_csv"]
            for table in tables:
                body2 = {
                    "db": DB_NAME,
                    "csl": f".show table {table} schema as json"
                }
                resp2 = await c.post(query_url, json=body2, headers=h)
                if resp2.status_code == 200:
                    data2 = resp2.json()
                    print(f"\n  === {table} ===")
                    import json
                    for frame in data2.get("Tables", []):
                        for row in frame.get("Rows", []):
                            try:
                                schema = json.loads(row[0])
                                cols = schema.get("OrderedColumns", [])
                                for col in cols:
                                    print(f"    {col['Name']}: {col['CslType']}")
                            except:
                                pass
        else:
            print(f"Error: {resp.status_code} {resp.text[:500]}")

asyncio.run(main())
