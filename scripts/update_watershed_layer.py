"""
update_watershed_layer.py
Geometry source: RMD_Watersheds (FeatureServer/1) — owned by HARC/Erin, public
Count target:   GBEP_Watersheds_Summary_Stats (ITEM_ID) — owned by BCGIS

Every run does a full upsert:
  - Watersheds that already exist in the summary layer → update project_count
  - Watersheds new in RMD_Watersheds → add with geometry + project_count
  - New watersheds are picked up automatically on next run, no manual step needed.

GitHub secrets required: ARCGIS_USERNAME, ARCGIS_PASSWORD
"""

import json, os, datetime, requests
from shapely.geometry import shape

# ── CONFIG ───────────────────────────────────────────────────────────────────
ARCGIS_ORG  = "https://www.arcgis.com"
ITEM_ID     = "f2417bfa0d0f4983900cc2a2c20a4146"  # GBEP_Watersheds_Summary_Stats
LAYER_INDEX = 0

SURVEY123_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "survey123_6ce0c22f05d74de6bd806994a23cbc63_results/FeatureServer/0/query"
)
# RMD_Watersheds — layer index 1, Name field matches existing script
WATERSHED_GEO_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "RMD_Watersheds/FeatureServer/1/query"
)

USERNAME = os.environ["ARCGIS_USERNAME"]
PASSWORD = os.environ["ARCGIS_PASSWORD"]
# ──────────────────────────────────────────────────────────────────────────────


def get_token():
    r = requests.post(
        "https://www.arcgis.com/sharing/rest/generateToken",
        data={"username": USERNAME, "password": PASSWORD,
              "referer": "https://www.arcgis.com", "expiration": 120, "f": "json"},
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
        params={"f": "json", "token": token}, timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    url = data.get("url")
    if not url:
        raise RuntimeError("Could not get service URL")
    return url.rstrip("/") + f"/{LAYER_INDEX}"


def fetch_all(url, params):
    all_features, offset = [], 0
    while True:
        p = dict(params)
        p["resultOffset"] = offset
        p["resultRecordCount"] = 1000
        r = requests.get(url, params=p, timeout=60)
        r.raise_for_status()
        features = r.json().get("features", [])
        all_features.extend(features)
        if len(features) < 1000:
            break
        offset += 1000
    return all_features


def build_counts(survey_features, watershed_features):
    project_geoms = []
    for f in survey_features:
        geom = f.get("geometry")
        if not geom:
            continue
        try:
            g = shape(geom)
            if not g.is_valid:
                g = g.buffer(0)
            if g.is_valid and not g.is_empty:
                project_geoms.append(g)
        except Exception as e:
            print(f"  WARNING: skipping project geometry — {e}")
    print(f"  {len(project_geoms)} funded project geometries loaded")

    shapes = []
    for f in watershed_features:
        name = f["properties"].get("Name")
        geom = f.get("geometry")
        if not name:
            print(f"  WARNING: watershed with no Name — skipping. Properties: {f['properties']}")
            continue
        if not geom:
            print(f"  WARNING: watershed '{name}' has no geometry — skipping")
            continue
        try:
            poly = shape(geom)
            if not poly.is_valid:
                print(f"  INFO: repairing geometry for '{name}'")
                poly = poly.buffer(0)
            if poly.is_valid and not poly.is_empty:
                shapes.append({"name": name, "shape": poly, "geom": geom})
            else:
                print(f"  WARNING: '{name}' still invalid after repair — skipping")
        except Exception as e:
            print(f"  WARNING: skipping watershed '{name}' — {e}")
    print(f"  {len(shapes)} watershed geometries loaded")

    # No break — project counts in every watershed it intersects.
    counts = {w["name"]: 0 for w in shapes}
    for g in project_geoms:
        for w in shapes:
            if w["shape"].intersects(g):
                counts[w["name"]] += 1

    total = sum(counts.values())
    print(f"  {sum(1 for c in counts.values() if c > 0)} watersheds with projects")
    print(f"  {total} total project-watershed assignments")
    return shapes, counts


def geom_to_rings(geom):
    if geom["type"] == "Polygon":
        return geom["coordinates"]
    elif geom["type"] == "MultiPolygon":
        rings = []
        for poly in geom["coordinates"]:
            rings.extend(poly)
        return rings
    return []


def upsert_features(service_url, token, shapes, counts):
    """
    Update project_count on existing watersheds by Name.
    Add geometry + project_count for any watershed not yet in the hosted layer.
    New watersheds added to RMD_Watersheds are picked up automatically.
    """
    r = requests.get(
        f"{service_url}/query",
        params={"where": "1=1", "outFields": "OBJECTID,Name",
                "f": "json", "token": token, "resultRecordCount": 1000},
        timeout=60,
    )
    r.raise_for_status()
    existing = r.json().get("features", [])
    existing_by_name = {
        f["attributes"]["Name"]: f["attributes"]["OBJECTID"]
        for f in existing if f["attributes"].get("Name")
    }
    print(f"  {len(existing_by_name)} watersheds currently in hosted layer")

    to_update, to_add = [], []
    for w in shapes:
        name  = w["name"]
        count = counts.get(name, 0)
        if name in existing_by_name:
            to_update.append({"attributes": {
                "OBJECTID": existing_by_name[name],
                "project_count": count,
            }})
        else:
            to_add.append({
                "geometry": {"rings": geom_to_rings(w["geom"]),
                             "spatialReference": {"wkid": 4326}},
                "attributes": {"Name": name, "project_count": count},
            })
    print(f"  {len(to_update)} to update, {len(to_add)} new to add")

    if to_update:
        for i in range(0, len(to_update), 100):
            batch = to_update[i:i+100]
            r = requests.post(f"{service_url}/updateFeatures",
                data={"features": json.dumps(batch), "rollbackOnFailure": "true",
                      "f": "json", "token": token}, timeout=60)
            r.raise_for_status()
            res = r.json()
            if "error" in res:
                raise RuntimeError(f"updateFeatures error: {res['error']}")
            failed = [x for x in res.get("updateResults", []) if not x.get("success")]
            if failed:
                print(f"  WARNING: {len(failed)} updates failed")
            else:
                print(f"  Updated batch {i//100+1}: {len(batch)} OK")

    if to_add:
        for i in range(0, len(to_add), 20):
            batch = to_add[i:i+20]
            r = requests.post(f"{service_url}/addFeatures",
                data={"features": json.dumps(batch), "rollbackOnFailure": "false",
                      "f": "json", "token": token}, timeout=120)
            r.raise_for_status()
            res = r.json()
            if "error" in res:
                raise RuntimeError(f"addFeatures error: {res['error']}")
            failed = [x for x in res.get("addResults", []) if not x.get("success")]
            if failed:
                print(f"  WARNING: {len(failed)} adds failed")
            else:
                print(f"  Added batch {i//20+1}: {len(batch)} OK")


def main():
    token = get_token()
    service_url = get_service_url(token)
    print(f"Service URL: {service_url}")

    print("Fetching Survey123 funded project polygons...")
    survey_features = fetch_all(SURVEY123_URL, {
        "where": "gbep_award_amount > 0", "outFields": "OBJECTID",
        "returnGeometry": "true", "outSR": "4326", "f": "geojson",
    })
    print(f"  {len(survey_features)} funded records fetched")

    print("Fetching RMD_Watersheds polygons...")
    ws_features = fetch_all(WATERSHED_GEO_URL, {
        "where": "1=1", "outFields": "Name",
        "outSR": "4326", "f": "geojson",
    })
    print(f"  {len(ws_features)} watersheds fetched")

    print("Running spatial join...")
    shapes, counts = build_counts(survey_features, ws_features)

    print("Upserting to hosted layer...")
    upsert_features(service_url, token, shapes, counts)

    out = {
        "updated": datetime.datetime.utcnow().isoformat() + "Z",
        "watersheds": [
            {"name": w["name"], "project_count": counts.get(w["name"], 0)}
            for w in sorted(shapes, key=lambda x: -counts.get(x["name"], 0))
        ],
    }
    with open("watershed_counts.json", "w") as f:
        json.dump(out, f, indent=2)
    print("watershed_counts.json updated. Done.")


if __name__ == "__main__":
    main()
