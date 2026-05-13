"""
apply_symbology.py

Two-step approach:
  Step 1 — updateDefinition on the feature service to set drawingInfo at the
            service level. AGOL respects service-level renderers more reliably
            than webmap-level overrides for Arcade-driven unique value renderers.

  Step 2 — Patch the webmap to REMOVE any layerDefinition.drawingInfo override
            on the NRU Projects layer, so the map falls back to the service
            default set in Step 1.

Categories:
  Land Acquisition                       -> solid red    #B03A3A
  Restoration, Stewardship & Research    -> solid orange #F0910C
  Land Acquisition with Additional Goals -> solid teal   #2d5f6b

GitHub secrets required: ARCGIS_USERNAME, ARCGIS_PASSWORD
"""

import os, json, requests

# ── CONFIG ────────────────────────────────────────────────────────────────────
LAYER_URL      = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "survey123_6ce0c22f05d74de6bd806994a23cbc63_results/FeatureServer/0"
)
WEBMAP_ITEM_ID = "3504d4f140f74acc8fe02c9231143709"
NRU_ITEM_ID    = "92bcdb290d7143d893b105f121c87ee7"
USERNAME       = os.environ["ARCGIS_USERNAME"]
PASSWORD       = os.environ["ARCGIS_PASSWORD"]
AGOL           = "https://www.arcgis.com"
# ──────────────────────────────────────────────────────────────────────────────

ARCADE = (
    "var g = IIf(IsEmpty($feature.edited_broad_project_goals), "
    "IIf(IsEmpty($feature.broad_project_goals), '', $feature.broad_project_goals), "
    "$feature.edited_broad_project_goals); "
    "var glo = Lower(g); "
    "var hasLA = Find('land acquisition', glo) > -1 || Find('land_acquisition', glo) > -1; "
    "var stripped = Trim(Replace(Replace(glo, 'land acquisition', ''), 'land_acquisition', '')); "
    "var hasOther = stripped != '' && stripped != ',' && Len(Replace(stripped, ',', '')) > 0; "
    "if (hasLA && hasOther) return 'Land Acquisition with Additional Goals'; "
    "if (hasLA) return 'Land Acquisition'; "
    "return 'Restoration, Stewardship, and Research';"
)

RENDERER = {
    "type": "uniqueValue",
    "valueExpression": ARCADE,
    "valueExpressionTitle": "Project Goal Category",
    "uniqueValueInfos": [
        {
            "value": "Land Acquisition",
            "label": "Land Acquisition",
            "symbol": {
                "type": "esriSFS", "style": "esriSFSSolid",
                "color": [176, 58, 58, 220],
                "outline": {"type":"esriSLS","style":"esriSLSSolid",
                            "color":[176,58,58,255],"width":1.5}
            }
        },
        {
            "value": "Restoration, Stewardship, and Research",
            "label": "Restoration, Stewardship & Research",
            "symbol": {
                "type": "esriSFS", "style": "esriSFSSolid",
                "color": [240, 145, 12, 220],
                "outline": {"type":"esriSLS","style":"esriSLSSolid",
                            "color":[240,145,12,255],"width":1.5}
            }
        },
        {
            "value": "Land Acquisition with Additional Goals",
            "label": "Land Acquisition with Additional Goals",
            "symbol": {
                "type": "esriSFS", "style": "esriSFSSolid",
                "color": [45, 95, 107, 220],
                "outline": {"type":"esriSLS","style":"esriSLSSolid",
                            "color":[45,95,107,255],"width":1.5}
            }
        }
    ],
    "defaultSymbol": {
        "type": "esriSFS", "style": "esriSFSSolid",
        "color": [150,150,150,150],
        "outline": {"type":"esriSLS","style":"esriSLSSolid",
                    "color":[100,100,100,200],"width":0.5}
    },
    "defaultLabel": "Unknown / No data"
}


def get_token():
    r = requests.post(
        f"{AGOL}/sharing/rest/generateToken",
        data={"username": USERNAME, "password": PASSWORD,
              "referer": AGOL, "expiration": 60, "f": "json"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"Token error: {data['error']}")
    print("Token acquired.")
    return data["token"]


# ── STEP 1: Update service drawingInfo ───────────────────────────────────────
def update_service_renderer(token):
    print("\nStep 1 — Applying renderer to feature service...")
    r = requests.post(
        f"{LAYER_URL}/updateDefinition",
        data={
            "updateDefinition": json.dumps({"drawingInfo": {"renderer": RENDERER}}),
            "f": "json",
            "token": token,
        },
        timeout=60,
    )
    r.raise_for_status()
    result = r.json()
    if "error" in result:
        print(f"  WARNING: updateDefinition returned error: {result['error']}")
        print("  The service may not support drawingInfo updates.")
        print("  Continuing to Step 2 anyway.")
    else:
        print("  Service renderer updated successfully.")


# ── STEP 2: Remove webmap renderer override ──────────────────────────────────
def clear_webmap_override(token):
    print("\nStep 2 — Removing webmap renderer override...")

    # Fetch current webmap JSON
    r = requests.get(
        f"{AGOL}/sharing/rest/content/items/{WEBMAP_ITEM_ID}/data",
        params={"f": "json", "token": token},
        timeout=30,
    )
    r.raise_for_status()
    webmap = r.json()
    if "error" in webmap:
        raise RuntimeError(f"Could not fetch webmap: {webmap['error']}")

    layers = webmap.get("operationalLayers", [])
    print(f"  Webmap has {len(layers)} top-level layers:")
    for lyr in layers:
        print(f"    - {lyr.get('title','(no title)')}  itemId={lyr.get('itemId','')}  url={lyr.get('url','')[:60]}")

    patched = False

    def walk(layer_list):
        nonlocal patched
        for layer in layer_list:
            match = (
                layer.get("itemId") == NRU_ITEM_ID
                or "survey123_6ce0c22f" in layer.get("url", "")
            )
            if match:
                title = layer.get("title", layer.get("url", "unknown"))
                if "layerDefinition" in layer and "drawingInfo" in layer["layerDefinition"]:
                    del layer["layerDefinition"]["drawingInfo"]
                    print(f"  Cleared drawingInfo override on: {title}")
                else:
                    print(f"  No override found on: {title} (already clean)")
                patched = True
            for key in ("layers", "operationalLayers"):
                if key in layer:
                    walk(layer[key])

    walk(layers)

    if not patched:
        print("  WARNING: NRU Projects layer not found in webmap by itemId or URL.")
        print("  Step 1 service update still applies — check if map shows correct colors.")
        return

    # Save webmap
    r = requests.post(
        f"{AGOL}/sharing/rest/content/users/{USERNAME}/items/{WEBMAP_ITEM_ID}/update",
        data={"text": json.dumps(webmap), "f": "json", "token": token},
        timeout=60,
    )
    r.raise_for_status()
    result = r.json()
    if result.get("success"):
        print("  Webmap saved — override cleared.")
    else:
        raise RuntimeError(f"Webmap save failed: {result}")


def main():
    token = get_token()
    update_service_renderer(token)
    clear_webmap_override(token)
    print("\nDone. Hard-refresh the Explore map (Ctrl+Shift+R) to see the updated symbology.")
    print("Do not open the Style panel in Map Viewer — it will overwrite the renderer.")


if __name__ == "__main__":
    main()
