"""Add data bindings to OrderToCash_Ontology for all 4 entity types."""
import asyncio
import json
import uuid
from src.fabric_client import FabricClient
from src.definition_utils import decode_definition, encode_definition

WS = "b4736da7-7744-4aad-87df-60e426ed2d30"
ONT = "2ec03773-3911-49a9-917e-aac4ec26d67e"
EH_ID = "531d8149-3f7a-4d28-a521-1aa8e283250f"
CLUSTER_URI = "https://trd-pxqduv45ps1nwvw094.z5.kusto.fabric.microsoft.com"
DB_NAME = "order_to_cash_eh"

# Column -> Entity property name mappings per entity + timestamp col
ORDER_BINDINGS = {
    "orders": {
        "mapping": {
            "order_id": "OrderId",
            "customer_id": "CustomerId",
            "plant_id": "PlantId",
            "variant_code": "VariantCode",
        },
        "timestamp": "order_created_ts",
        "ts_prop": "OrderCreatedTs",
    },
    "object_current_state": {
        "mapping": {
            "current_status": "CurrentStatus",
            "object_id": "OrderId",
        },
        "timestamp": "last_state_ts",
        "ts_prop": "LastStateTs",
    },
    "sla_metrics": {
        "mapping": {
            "order_id": "OrderId",
            "cycle_time_hours": "CycleTimeHours",
            "is_sla_breached": "IsSLABreached",
            "breach_hours": "BreachHours",
        },
        "timestamp": "case_start_ts",
        "ts_prop": "CaseStartTs",
    },
}

VARIANT_BINDINGS = {
    "variant_metrics": {
        "mapping": {
            "variant_code": "VariantCode",
            "variant_name": "VariantName",
            "activity_path": "ActivityPath",
            "activity_count": "ActivityCount",
            "case_count": "CaseCount",
            "avg_cycle_time_hours": "AvgCycleTimeHours",
            "avg_manual_touch_count": "AvgManualTouchCount",
            "conformance_rate": "ConformanceRate",
        },
        "timestamp": None,
    },
}

PLANT_BINDINGS = {
    "plants": {
        "mapping": {
            "plant_id": "PlantId",
            "plant_name": "PlantName",
            "region": "Region",
        },
        "timestamp": None,
    },
}

CUSTOMER_BINDINGS = {
    "customers": {
        "mapping": {
            "customer_id": "CustomerId",
            "segment": "Segment",
            "region": "Region",
            "risk_tier": "RiskTier",
        },
        "timestamp": "created_date",
        "ts_prop": "CreatedDate",
    },
}


def find_entity_by_name(decoded, name):
    for eid, edata in decoded["entityTypes"].items():
        if edata["definition"]["name"] == name:
            return eid, edata
    return None, None


def find_prop_id(entity_data, prop_name):
    defn = entity_data["definition"]
    for p in defn.get("properties", []):
        if p["name"] == prop_name:
            return p["id"]
    for p in defn.get("timeseriesProperties", []):
        if p["name"] == prop_name:
            return p["id"]
    return None


def make_kusto_binding(table_name, col_to_prop_map, entity_data, timestamp_col=None):
    bindings = []
    for col_name, prop_name in col_to_prop_map.items():
        prop_id = find_prop_id(entity_data, prop_name)
        if prop_id:
            bindings.append({
                "sourceColumnName": col_name,
                "targetPropertyId": str(prop_id)
            })
    
    binding_type = "TimeSeries" if timestamp_col else "NonTimeSeries"
    config = {
        "dataBindingType": binding_type,
        "propertyBindings": bindings,
        "sourceTableProperties": {
            "sourceType": "KustoTable",
            "workspaceId": WS,
            "itemId": EH_ID,
            "clusterUri": CLUSTER_URI,
            "databaseName": DB_NAME,
            "sourceTableName": table_name,
        }
    }
    if timestamp_col:
        config["timestampColumnName"] = timestamp_col

    return {
        "id": str(uuid.uuid4()),
        "dataBindingConfiguration": config
    }


import random

def gid():
    return str(random.randint(10**12, 2**53))

def add_ts_property(entity_data, ts_prop_name):
    """Add a timeseries DateTime property if not already present."""
    defn = entity_data["definition"]
    ts_props = defn.setdefault("timeseriesProperties", [])
    for p in ts_props:
        if p["name"] == ts_prop_name:
            return p["id"]
    pid = gid()
    ts_props.append({
        "id": pid, "name": ts_prop_name, "valueType": "DateTime",
        "redefines": None, "baseTypeNamespaceType": None
    })
    return pid

def add_bindings_for_entity(entity_data, bindings_spec, entity_name):
    added = []
    for table, spec in bindings_spec.items():
        mapping = spec["mapping"]
        timestamp = spec.get("timestamp")
        ts_prop = spec.get("ts_prop")

        if timestamp and ts_prop:
            # Add timeseries property for the timestamp column
            ts_pid = add_ts_property(entity_data, ts_prop)
            # Add timestamp column to the mapping
            full_mapping = dict(mapping)
            full_mapping[timestamp] = ts_prop
            db = make_kusto_binding(table, full_mapping, entity_data, timestamp_col=timestamp)
        elif timestamp:
            db = make_kusto_binding(table, mapping, entity_data, timestamp_col=timestamp)
        else:
            # No timestamp → skip KustoTable (NonTimeSeries not allowed)
            print(f"  SKIP {table} (no datetime column, KustoTable requires TimeSeries)")
            continue

        entity_data.setdefault("dataBindings", []).append(db)
        added.append(table)
        print(f"  + {table} ({len(mapping)} columns, ts={timestamp})")
    return added


async def main():
    c = FabricClient()

    # Get current definition
    raw = await c.get_ontology_definition(WS, ONT)
    parts = raw.get("definition", {}).get("parts", [])
    decoded = decode_definition(parts)

    # Order bindings
    order_id, order_data = find_entity_by_name(decoded, "Order")
    if order_data:
        print(f"Order ({order_id}):")
        add_bindings_for_entity(order_data, ORDER_BINDINGS, "Order")

    # Variant bindings
    variant_id, variant_data = find_entity_by_name(decoded, "Variant")
    if variant_data:
        print(f"Variant ({variant_id}):")
        add_bindings_for_entity(variant_data, VARIANT_BINDINGS, "Variant")

    # Plant bindings
    plant_id, plant_data = find_entity_by_name(decoded, "Plant")
    if plant_data:
        print(f"Plant ({plant_id}):")
        add_bindings_for_entity(plant_data, PLANT_BINDINGS, "Plant")

    # Customer bindings
    cust_id, cust_data = find_entity_by_name(decoded, "Customer")
    if cust_data:
        print(f"Customer ({cust_id}):")
        add_bindings_for_entity(cust_data, CUSTOMER_BINDINGS, "Customer")

    # Push updated definition
    print("\nPushing updated definition...")
    new_parts = encode_definition(decoded)
    await c.update_ontology_definition(WS, ONT, {"parts": new_parts})
    print("Push accepted!")

    # Wait and verify
    import time
    time.sleep(5)
    raw2 = await c.get_ontology_definition(WS, ONT)
    parts2 = raw2.get("definition", {}).get("parts", [])
    decoded2 = decode_definition(parts2)

    print("\n=== Verification ===")
    for eid, edata in decoded2["entityTypes"].items():
        name = edata["definition"]["name"]
        dbs = edata.get("dataBindings", [])
        print(f"  {name}: {len(dbs)} data binding(s)")
        for db in dbs:
            cfg = db.get("dataBindingConfiguration", {})
            src = cfg.get("sourceTableProperties", {})
            binds = cfg.get("propertyBindings", [])
            print(f"    - {src.get('sourceTableName','?')}: {len(binds)} column bindings")

    await c.close()

asyncio.run(main())
