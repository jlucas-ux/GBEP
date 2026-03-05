"""
update_watershed_layer.py
Fetches Survey123 project points, spatially joins them to GBEP watershed polygons,
counts projects per watershed, and updates a 'project_count' field on a
hosted ArcGIS Online feature layer.

Requires GitHub secrets:
  ARCGIS_USERNAME  - ArcGIS Online username
  ARCGIS_PASSWORD  - ArcGIS Online password

The target hosted feature layer item ID is hardcoded below.
"""

import json
import os
import requests
from shapely.geometry import shape, Point

# ── CONFIG ────────────────────────────────────────────────────────────────────
ARCGIS_ORG        = "https://www.arcgis.com"
ITEM_ID           = "11bb8b1c2ab246b59460dbee20aa2343"
LAYER_INDEX       = 0  # first layer in the service

SURVEY123_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "survey123_6ce0c22f05d74de6bd806994a23cbc63_results/FeatureServer/0/query"
)
WATERSHED_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "GBEP_Watersheds_Summary_Stats/FeatureServer/0/query"
)

USERNAME = os.environ["ARCGIS_USERNAME"]
PASSWORD = os.environ["ARCGIS_PASSWORD"]
# ──────────────────────────────────────────────────────────────────────────────


def get_token(username, password):
    """Get a short-lived ArcGIS Online token."""
    r = requests.post(
        "https://www.arcgis.com/sharing/rest/generateToken",
        data={
            "username": username,
            "password": password,
            "referer": "https://www.arcgis.com",
            "expiration": 60,
            "f": "json",
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"Token error: {data['error']}")
    print("Token acquired.")
    return data["token"]


def get_service_url(item_id, token):
    """Get the feature service URL from the item ID."""
    r = requests.get(
        f"{ARCGIS_ORG}/sharing/rest/content/items/{item_id}",
        params={"f": "json", "token": token},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    url = data.get("url")
    if not url:
        raise RuntimeError(f"Could not get service URL for item {item_id}")
    return url.rstrip("/") + f"/{LAYER_INDEX}"


def ensure_project_count_field(service_url, token):
    """Add project_count field to layer if it doesn't exist yet."""
    # Check existing fields
    r = requests.get(
        service_url,
        params={"f": "json", "token": token},
        timeout=30,
    )
    r.raise_for_status()
    info = r.json()
    existing = [f["name"].lower() for f in info.get("fields", [])]

    if "project_count" in existing:
        print("project_count field already exists.")
        return

    print("Adding project_count field...")
    add_url = service_url.replace(f"/{LAYER_INDEX}", "") + "/addToDefinition"
    payload = {
        "layers": [
            {
                "id": LAYER_INDEX,
                "fields": [
                    {
                        "name": "project_count",
                        "type": "esriFieldTypeInteger",
                        "alias": "Project Count",
                        "nullable": True,
                        "editable": True,
                        "defaultValue": 0,
                    }
                ],
            }
        ]
    }
    r = requests.post(
        add_url,
        data={"addToDefinition": json.dumps(payload), "f": "json", "token": token},
        timeout=30,
    )
    r.raise_for_status()
    result = r.json()
    if "error" in result:
        raise RuntimeError(f"Failed to add field: {result['error']}")
    print("project_count field added.")


def fetch_all_features(url, params):
    """Fetch all features with pagination."""
    all_features = []
    offset = 0
    while True:
        p = dict(params)
        p["resultOffset"] = offset
        p["resultRecordCount"] = 1000
        r = requests.get(url, params=p, timeout=60)
        r.raise_for_status()
        data = r.json()
        features = data.get("features", [])
        all_features.extend(features)
        if len(features) < 1000:
            break
        offset += 1000
    return all_features


def main():
    # ── Auth ──
    token = get_token(USERNAME, PASSWORD)
    service_url = get_service_url(ITEM_ID, token)
    print(f"Service URL: {service_url}")

    # ── Ensure field exists ──
    ensure_project_count_field(service_url, token)

    # ── Fetch Survey123 points ──
    print("Fetching Survey123 project points...")
    survey_features = fetch_all_features(SURVEY123_URL, {
        "where": "gbep_award_amount > 0",
        "outFields": "OBJECTID",
        "outSR": "4326",
        "f": "geojson",
    })
    print(f"  {len(survey_features)} funded project records")

    points = []
    for f in survey_features:
        geom = f.get("geometry")
        if not geom:
            continue
        try:
            pt = shape(geom)
            if pt.is_valid and not pt.is_empty:
                points.append(pt)
        except Exception:
            continue
    print(f"  {len(points)} with valid geometry")

    # ── Fetch watershed features with OBJECTIDs ──
    print("Fetching watershed features...")
    ws_features = fetch_all_features(WATERSHED_URL, {
        "where": "1=1",
        "outFields": "Name,OBJECTID",
        "outSR": "4326",
        "f": "geojson",
    })
    print(f"  {len(ws_features)} watersheds")

    # ── Spatial join ──
    print("Running spatial join...")
    counts = {}
    shapes = []
    for f in ws_features:
        name = f["properties"].get("Name")
        oid  = f["properties"].get("OBJECTID")
        geom = f.get("geometry")
        if not name or not oid or not geom:
            continue
        try:
            poly = shape(geom)
            if poly.is_valid and not poly.is_empty:
                shapes.append({"name": name, "oid": oid, "shape": poly})
                counts[oid] = 0
        except Exception:
            continue

    for pt in points:
        for w in shapes:
            if w["shape"].contains(pt):
                counts[w["oid"]] += 1
                break

    total = sum(counts.values())
    print(f"  Total projects assigned: {total}")

    # ── Build update features ──
    updates = []
    for w in shapes:
        updates.append({
            "attributes": {
                "OBJECTID": w["oid"],
                "project_count": counts.get(w["oid"], 0),
            }
        })

    # ── Push updates in batches of 100 ──
    print("Updating hosted feature layer...")
    batch_size = 100
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i+batch_size]
        r = requests.post(
            f"{service_url}/updateFeatures",
            data={
                "features": json.dumps(batch),
                "rollbackOnFailure": "true",
                "f": "json",
                "token": token,
            },
            timeout=60,
        )
        r.raise_for_status()
        result = r.json()
        if "error" in result:
            raise RuntimeError(f"Update failed: {result['error']}")
        results = result.get("updateResults", [])
        failed = [x for x in results if not x.get("success")]
        if failed:
            print(f"  WARNING: {len(failed)} features failed to update")
        else:
            print(f"  Batch {i//batch_size+1}: {len(batch)} features updated OK")

    # ── Also write watershed_counts.json for the D3 map fallback ──
    import datetime
    out = {
        "updated": datetime.datetime.utcnow().isoformat() + "Z",
        "watersheds": [
            {"name": w["name"], "project_count": counts.get(w["oid"], 0)}
            for w in sorted(shapes, key=lambda x: -counts.get(x["oid"], 0))
        ],
    }
    with open("watershed_counts.json", "w") as f:
        json.dump(out, f, indent=2)
    print("watershed_counts.json also updated.")
    print("Done.")


if __name__ == "__main__":
    main()
