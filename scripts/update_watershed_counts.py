"""
update_watershed_counts.py
Fetches Survey123 project points and GBEP watershed polygons from ArcGIS Online,
performs a spatial join (point-in-polygon), counts projects per watershed,
and writes the result to watershed_counts.json in the repo root.
"""

import json
import requests
from shapely.geometry import shape, Point

SURVEY123_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "survey123_6ce0c22f05d74de6bd806994a23cbc63_results/FeatureServer/0/query"
)

WATERSHED_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "GBEP_Watersheds_Summary_Stats/FeatureServer/0/query"
)


def fetch_all_features(url, params):
    """Fetch all features from an ArcGIS REST endpoint, handling pagination."""
    all_features = []
    offset = 0
    page_size = 1000
    while True:
        p = dict(params)
        p["resultOffset"] = offset
        p["resultRecordCount"] = page_size
        r = requests.get(url, params=p, timeout=60)
        r.raise_for_status()
        data = r.json()
        features = data.get("features", [])
        all_features.extend(features)
        if len(features) < page_size:
            break
        offset += page_size
    return all_features


def main():
    print("Fetching Survey123 project points...")
    survey_params = {
        "where": "gbep_award_amount > 0",
        "outFields": "gbep_award_amount,numeric_approximate_project_siz",
        "outSR": "4326",
        "f": "geojson",
    }
    survey_features = fetch_all_features(SURVEY123_URL, survey_params)
    print(f"  Found {len(survey_features)} funded project records")

    # Build list of valid points
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
    print(f"  {len(points)} records have valid geometry")

    print("Fetching watershed polygons...")
    watershed_params = {
        "where": "1=1",
        "outFields": "Name",
        "outSR": "4326",
        "f": "geojson",
    }
    watershed_features = fetch_all_features(WATERSHED_URL, watershed_params)
    print(f"  Found {len(watershed_features)} watersheds")

    # Build watershed shapes
    watersheds = []
    for f in watershed_features:
        name = f.get("properties", {}).get("Name")
        geom = f.get("geometry")
        if not name or not geom:
            continue
        try:
            poly = shape(geom)
            if poly.is_valid and not poly.is_empty:
                watersheds.append({"name": name, "shape": poly})
        except Exception:
            continue

    print("Running spatial join (point-in-polygon)...")
    counts = {w["name"]: 0 for w in watersheds}

    for pt in points:
        for w in watersheds:
            if w["shape"].contains(pt):
                counts[w["name"]] += 1
                break  # each project counts once

    # Build output
    result = {
        "updated": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "watersheds": [
            {"name": name, "project_count": count}
            for name, count in sorted(counts.items(), key=lambda x: -x[1])
        ],
    }

    out_path = "watershed_counts.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Written to {out_path}")
    total = sum(counts.values())
    print(f"Total projects assigned to a watershed: {total}")
    assigned = sum(1 for v in counts.values() if v > 0)
    print(f"Watersheds with at least 1 project: {assigned}/{len(watersheds)}")


if __name__ == "__main__":
    main()
