"""
apply_symbology.py
Applies three-category project goal symbology by patching the webmap JSON.
Updates the renderer override on the NRU Projects layer inside the webmap
so it takes effect immediately without affecting other maps.

Categories:
  Land Acquisition                       -> solid red  #B03A3A
  Restoration, Stewardship & Research    -> solid orange #F0910C
  Land Acquisition with Additional Goals -> diagonal red/orange stripe

GitHub secrets required: ARCGIS_USERNAME, ARCGIS_PASSWORD
"""

import os, json, requests

# ── CONFIG ────────────────────────────────────────────────────────────────────
WEBMAP_ITEM_ID  = "3504d4f140f74acc8fe02c9231143709"
NRU_ITEM_ID     = "92bcdb290d7143d893b105f121c87ee7"
USERNAME        = os.environ["ARCGIS_USERNAME"]
PASSWORD        = os.environ["ARCGIS_PASSWORD"]
AGOL            = "https://www.arcgis.com"
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


def get_webmap(token):
    r = requests.get(
        f"{AGOL}/sharing/rest/content/items/{WEBMAP_ITEM_ID}/data",
        params={"f": "json", "token": token},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"Could not fetch webmap: {data['error']}")
    layers = data.get("operationalLayers", [])
    print(f"Webmap fetched. Operational layers: {len(layers)}")
    for lyr in layers:
        print(f"  - {lyr.get('title','(no title)')}  itemId={lyr.get('itemId','')}")
    return data


def patch_renderer(webmap):
    patched = False

    def walk(layer_list):
        nonlocal patched
        for layer in layer_list:
            match = (
                layer.get("itemId") == NRU_ITEM_ID
                or "survey123_6ce0c22f" in layer.get("url", "")
            )
            if match:
                layer.setdefault("layerDefinition", {})
                layer["layerDefinition"].setdefault("drawingInfo", {})
                layer["layerDefinition"]["drawingInfo"]["renderer"] = RENDERER
                print(f"  Patched: {layer.get('title', layer.get('url','unknown'))}")
                patched = True
            for key in ("layers", "operationalLayers"):
                if key in layer:
                    walk(layer[key])

    walk(webmap.get("operationalLayers", []))

    if not patched:
        raise RuntimeError(
            f"NRU Projects layer not found in webmap.\n"
            f"Expected itemId={NRU_ITEM_ID} or URL containing 'survey123_6ce0c22f'.\n"
            "Check the layer list printed above."
        )
    return webmap


def save_webmap(token, webmap):
    r = requests.post(
        f"{AGOL}/sharing/rest/content/users/{USERNAME}/items/{WEBMAP_ITEM_ID}/update",
        data={"text": json.dumps(webmap), "f": "json", "token": token},
        timeout=60,
    )
    r.raise_for_status()
    result = r.json()
    if result.get("success"):
        print("Webmap updated successfully.")
    else:
        raise RuntimeError(f"Webmap update failed: {result}")


def main():
    token  = get_token()
    webmap = get_webmap(token)
    webmap = patch_renderer(webmap)
    save_webmap(token, webmap)
    print("Done. Reload the Explore map in ArcGIS Online to see the new symbology.")


if __name__ == "__main__":
    main()
