"""
update_watershed_layer.py
- First run: fetches watershed polygons from source layer, does spatial join
  with Survey123 points, and ADDS all features (with geometry) to the new
  hosted layer.
- Subsequent runs: updates project_count on existing features by Name.

GitHub secrets required:
  ARCGIS_USERNAME
  ARCGIS_PASSWORD
"""

import json
import os
import datetime
import requests
from shapely.geometry import shape, Point

# ── CONFIG ────────────────────────────────────────────────────────────────────
ARCGIS_ORG  = "https://www.arcgis.com"
ITEM_ID     = "f2417bfa0d0f4983900cc2a2c20a4146"
LAYER_INDEX = 0

SURVEY123_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "survey123_6ce0c22f05d74de6bd806994a23cbc63_results/FeatureServer/0/query"
)
WATERSHED_GEO_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "GBEP_Watersheds_Summary_Stats/FeatureServer/0/query"
)

USERNAME = os.environ["ARCGIS_USERNAME"]
PASSWORD = os.environ["ARCGIS_PASSWORD"]
# ──────────────────────────────────────────────────────────────────────────────


def get_token():
    r = requests.post(
        "https://www.arcgis.com/sharing/rest/generateToken",
        data={
            "username": USERNAME,
            "password": PASSWORD,
            "referer": "https://www.arcgis.com",
            "expiration": 120,
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


def get_service_url(token):
    r = requests.get(
        f"{ARCGIS_ORG}/sharing/rest/content/items/{ITEM_ID}",
        params={"f": "json", "token": token},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    url = data.get("url")
    if not url:
        raise RuntimeError("Could not get service URL")
    return url.rstrip("/") + f"/{LAYER_INDEX}"


def fetch_all(url, params):
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


def get_existing_count(service_url, token):
    r = requests.get(
        f"{service_url}/query",
        params={"where": "1=1", "returnCountOnly": "true", "f": "json", "token": token},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("count", 0)


def build_counts(survey_features, watershed_features):
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
    print(f"  {len(points)} funded project points with geometry")

    shapes = []
    for f in watershed_features:
        name = f["properties"].get("Name")
        geom = f.get("geometry")
        if not name or not geom:
            continue
        try:
            poly = shape(geom)
            if poly.is_valid and not poly.is_empty:
                shapes.append({"name": name, "shape": poly, "geom": geom})
        except Exception:
            continue

    counts = {w["name"]: 0 for w in shapes}
    for pt in points:
        for w in shapes:
            if w["shape"].contains(pt):
                counts[w["name"]] += 1
                break

    return shapes, counts


def geom_to_rings(geom):
    """Convert GeoJSON geometry to ArcGIS rings format."""
    if geom["type"] == "Polygon":
        return geom["coordinates"]
    elif geom["type"] == "MultiPolygon":
        rings = []
        for poly in geom["coordinates"]:
            rings.extend(poly)
        return rings
    return []


def add_features(service_url, token, shapes, counts):
    print(f"Adding {len(shapes)} features to hosted layer...")
    features = []
    for w in shapes:
        rings = geom_to_rings(w["geom"])
        features.append({
            "geometry": {
                "rings": rings,
                "spatialReference": {"wkid": 4326}
            },
            "attributes": {
                "Name": w["name"],
                "project_count": counts.get(w["name"], 0),
            }
        })

    batch_size = 20
    for i in range(0, len(features), batch_size):
        batch = features[i:i+batch_size]
        r = requests.post(
            f"{service_url}/addFeatures",
            data={
                "features": json.dumps(batch),
                "rollbackOnFailure": "false",
                "f": "json",
                "token": token,
            },
            timeout=120,
        )
        r.raise_for_status()
        result = r.json()
        if "error" in result:
            raise RuntimeError(f"addFeatures error: {result['error']}")
        failed = [x for x in result.get("addResults", []) if not x.get("success")]
        if failed:
            print(f"  WARNING: {len(failed)} features failed in batch {i//batch_size+1}")
            for ff in failed:
                print(f"    {ff}")
        else:
            print(f"  Batch {i//batch_size+1}: {len(batch)} features added OK")


def update_features(service_url, token, counts):
    print("Fetching existing OBJECTIDs from hosted layer...")
    r = requests.get(
        f"{service_url}/query",
        params={
            "where": "1=1",
            "outFields": "OBJECTID,Name",
            "f": "json",
            "token": token,
            "resultRecordCount": 1000,
        },
        timeout=60,
    )
    r.raise_for_status()
    existing = r.json().get("features", [])

    updates = []
    for f in existing:
        name = f["attributes"].get("Name")
        oid  = f["attributes"].get("OBJECTID")
        if name and oid:
            updates.append({
                "attributes": {
                    "OBJECTID": oid,
                    "project_count": counts.get(name, 0),
                }
            })

    print(f"Updating {len(updates)} features...")
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
            raise RuntimeError(f"updateFeatures error: {result['error']}")
        failed = [x for x in result.get("updateResults", []) if not x.get("success")]
        if failed:
            print(f"  WARNING: {len(failed)} failed in batch {i//batch_size+1}")
        else:
            print(f"  Batch {i//batch_size+1}: {len(batch)} updated OK")


def main():
    token = get_token()
    service_url = get_service_url(token)
    print(f"Service URL: {service_url}")

    print("Fetching Survey123 funded project points...")
    survey_features = fetch_all(SURVEY123_URL, {
        "where": "gbep_award_amount > 0",
        "outFields": "OBJECTID",
        "outSR": "4326",
        "f": "geojson",
    })
    print(f"  {len(survey_features)} funded records")

    print("Fetching watershed polygons...")
    ws_features = fetch_all(WATERSHED_GEO_URL, {
        "where": "1=1",
        "outFields": "Name",
        "outSR": "4326",
        "f": "geojson",
    })
    print(f"  {len(ws_features)} watersheds")

    print("Running spatial join...")
    shapes, counts = build_counts(survey_features, ws_features)
    total = sum(counts.values())
    print(f"  Total projects assigned to a watershed: {total}")

    existing_count = get_existing_count(service_url, token)
    print(f"  Existing features in hosted layer: {existing_count}")

    if existing_count == 0:
        add_features(service_url, token, shapes, counts)
    else:
        update_features(service_url, token, counts)

    # Write watershed_counts.json for D3 map fallback
    out = {
        "updated": datetime.datetime.utcnow().isoformat() + "Z",
        "watersheds": [
            {"name": w["name"], "project_count": counts.get(w["name"], 0)}
            for w in sorted(shapes, key=lambda x: -counts.get(x["name"], 0))
        ],
    }
    with open("watershed_counts.json", "w") as f:
        json.dump(out, f, indent=2)
    print("watershed_counts.json updated.")
    print("Done.")


if __name__ == "__main__":
    main()
