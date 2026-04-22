"""Find Eventhouse details in Process Mining workspace."""
import asyncio
import httpx
from src.auth import get_access_token

WS = "b4736da7-7744-4aad-87df-60e426ed2d30"

async def main():
    token = await get_access_token()
    h = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=60) as c:
        resp = await c.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items", headers=h)
        items = resp.json().get("value", [])
        print(f"Total items in Process Mining workspace: {len(items)}")
        print()
        for item in items:
            itype = item.get("type", "")
            name = item.get("displayName", "")
            iid = item.get("id", "")
            if itype in ("Eventhouse", "KQLDatabase", "Lakehouse"):
                print(f"  {itype}: {name} (id={iid})")

        # Find Eventhouse specifically
        eventhouses = [i for i in items if i.get("type") == "Eventhouse"]
        if eventhouses:
            eh = eventhouses[0]
            eh_id = eh["id"]
            eh_name = eh["displayName"]
            print(f"\n--- Eventhouse: {eh_name} ---")
            print(f"  ID: {eh_id}")

            # Get KQL databases for this eventhouse
            kql_dbs = [i for i in items if i.get("type") == "KQLDatabase"]
            for db in kql_dbs:
                db_id = db["id"]
                db_name = db["displayName"]
                print(f"\n  KQL Database: {db_name} (id={db_id})")

                # Query database properties to get cluster URI
                resp2 = await c.get(
                    f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/kqlDatabases/{db_id}",
                    headers=h
                )
                if resp2.status_code == 200:
                    db_data = resp2.json()
                    props = db_data.get("properties", {})
                    cluster_uri = props.get("queryServiceUri", "")
                    db_real_name = props.get("databaseName", db_name)
                    parent = props.get("parentEventhouseItemId", "")
                    print(f"    Cluster URI: {cluster_uri}")
                    print(f"    Database Name: {db_real_name}")
                    print(f"    Parent Eventhouse: {parent}")

asyncio.run(main())
