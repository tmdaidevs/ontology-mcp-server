"""Push full Order-to-Cash ontology definition."""
import asyncio
import random
from src.fabric_client import FabricClient
from src.definition_utils import encode_definition

WS = "b4736da7-7744-4aad-87df-60e426ed2d30"
ONT = "2ec03773-3911-49a9-917e-aac4ec26d67e"

def gid():
    return str(random.randint(10**12, 2**53))

def make_props(specs):
    return [{"id": gid(), "name": n, "valueType": v, "redefines": None, "baseTypeNamespaceType": None} for n, v in specs]

def make_entity(eid, name, props):
    return {
        "id": eid, "namespace": "usertypes", "baseEntityTypeId": None,
        "name": name, "entityIdParts": [props[0]["id"]],
        "displayNamePropertyId": props[0]["id"],
        "namespaceType": "Custom", "visibility": "Visible",
        "properties": props, "timeseriesProperties": []
    }

async def main():
    c = FabricClient()

    # Order
    order_id = gid()
    order_props = make_props([
        ("OrderId", "String"), ("CustomerId", "String"), ("PlantId", "String"),
        ("VariantCode", "String"), ("CurrentStatus", "String"),
        ("CycleTimeHours", "Double"), ("IsSLABreached", "Boolean"),
        ("BreachHours", "Double"), ("ReworkCount", "BigInt"),
        ("ManualTouchCount", "BigInt"), ("HasReturn", "Boolean"), ("HasReplan", "Boolean")
    ])

    # Variant
    variant_id = gid()
    variant_props = make_props([
        ("VariantCode", "String"), ("VariantName", "String"), ("ActivityPath", "String"),
        ("ActivityCount", "BigInt"), ("CaseCount", "BigInt"),
        ("AvgCycleTimeHours", "Double"), ("AvgManualTouchCount", "Double"), ("ConformanceRate", "Double")
    ])

    # Plant
    plant_id = gid()
    plant_props = make_props([
        ("PlantId", "String"), ("PlantName", "String"), ("Region", "String"),
        ("AvgCycleTimeHours", "Double"), ("BreachRate", "Double")
    ])

    # Customer
    cust_id = gid()
    cust_props = make_props([
        ("CustomerId", "String"), ("Segment", "String"), ("Region", "String"), ("RiskTier", "String")
    ])

    rel1_id, rel2_id, rel3_id = gid(), gid(), gid()

    decoded = {
        "platform": {"metadata": {"type": "Ontology", "displayName": "OrderToCash_Ontology"}},
        "definition": {},
        "entityTypes": {
            order_id: {"definition": make_entity(order_id, "Order", order_props), "dataBindings": [], "documents": [], "overviews": {}, "resourceLinks": {}},
            variant_id: {"definition": make_entity(variant_id, "Variant", variant_props), "dataBindings": [], "documents": [], "overviews": {}, "resourceLinks": {}},
            plant_id: {"definition": make_entity(plant_id, "Plant", plant_props), "dataBindings": [], "documents": [], "overviews": {}, "resourceLinks": {}},
            cust_id: {"definition": make_entity(cust_id, "Customer", cust_props), "dataBindings": [], "documents": [], "overviews": {}, "resourceLinks": {}},
        },
        "relationshipTypes": {
            rel1_id: {"definition": {"namespace": "usertypes", "id": rel1_id, "name": "OrderToCustomer", "namespaceType": "Custom", "source": {"entityTypeId": order_id}, "target": {"entityTypeId": cust_id}}, "contextualizations": []},
            rel2_id: {"definition": {"namespace": "usertypes", "id": rel2_id, "name": "OrderToPlant", "namespaceType": "Custom", "source": {"entityTypeId": order_id}, "target": {"entityTypeId": plant_id}}, "contextualizations": []},
            rel3_id: {"definition": {"namespace": "usertypes", "id": rel3_id, "name": "OrderToVariant", "namespaceType": "Custom", "source": {"entityTypeId": order_id}, "target": {"entityTypeId": variant_id}}, "contextualizations": []},
        }
    }

    parts = encode_definition(decoded)
    print(f"Pushing {len(parts)} definition parts...")
    await c.update_ontology_definition(WS, ONT, {"parts": parts})
    print("Push accepted, waiting for LRO...")

    # Verify
    import time
    time.sleep(5)
    raw = await c.get_ontology_definition(WS, ONT)
    from src.definition_utils import decode_definition
    result_parts = raw.get("definition", {}).get("parts", [])
    result = decode_definition(result_parts)

    print(f"\nVerification - Entity Types: {len(result['entityTypes'])}")
    for eid, edata in result["entityTypes"].items():
        d = edata["definition"]
        print(f"  {d['name']} ({eid}): {len(d.get('properties',[]))} properties")
    print(f"\nRelationships: {len(result['relationshipTypes'])}")
    for rid, rdata in result["relationshipTypes"].items():
        d = rdata["definition"]
        print(f"  {d['name']}: {d['source']['entityTypeId']} -> {d['target']['entityTypeId']}")

    await c.close()

asyncio.run(main())
