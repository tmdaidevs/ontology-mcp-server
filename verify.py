import asyncio, json
from src.fabric_client import FabricClient
from src.definition_utils import decode_definition

async def verify():
    c = FabricClient()
    raw = await c.get_ontology_definition(
        "b4736da7-7744-4aad-87df-60e426ed2d30",
        "2ec03773-3911-49a9-917e-aac4ec26d67e"
    )
    parts = raw.get("definition", {}).get("parts", [])
    decoded = decode_definition(parts)
    
    print("Entity Types:")
    for eid, edata in decoded["entityTypes"].items():
        d = edata["definition"]
        props = d.get("properties", [])
        name = d["name"]
        print(f"  {name} ({eid}): {len(props)} properties")
        for p in props:
            print(f"    - {p['name']}: {p['valueType']}")
    
    print("\nRelationships:")
    for rid, rdata in decoded["relationshipTypes"].items():
        d = rdata["definition"]
        print(f"  {d['name']}: source={d['source']['entityTypeId']} -> target={d['target']['entityTypeId']}")
    
    await c.close()

asyncio.run(verify())
