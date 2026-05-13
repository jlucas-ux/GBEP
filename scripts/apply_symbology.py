"""
apply_symbology.py
Applies the three-category project goal symbology to the NRU Projects
feature service layer.

Run via GitHub Actions workflow dispatch, or locally with env vars set.
  ARCGIS_USERNAME and ARCGIS_PASSWORD must be set.

This updates the layer's drawingInfo (display only — no data is changed).
"""

import os, json, requests

LAYER_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "survey123_6ce0c22f05d74de6bd806994a23cbc63_results/FeatureServer/0"
)
USERNAME = os.environ["ARCGIS_USERNAME"]
PASSWORD = os.environ["ARCGIS_PASSWORD"]

RENDERER = {
  "type": "uniqueValue",
  "valueExpression": "var g = IIf(IsEmpty($feature.edited_broad_project_goals), IIf(IsEmpty($feature.broad_project_goals), '', $feature.broad_project_goals), $feature.edited_broad_project_goals); var glo = Lower(g); var hasLA = Find('land acquisition', glo) > -1 || Find('land_acquisition', glo) > -1; var stripped = Trim(Replace(Replace(glo, 'land acquisition', ''), 'land_acquisition', '')); var hasOther = stripped != '' && stripped != ',' && Len(Replace(stripped, ',', '')) > 0; if (hasLA && hasOther) return 'Both'; if (hasLA) return 'Land Acquisition'; return 'Restoration, Stewardship, and Research';",
  "valueExpressionTitle": "Project Goal Category",
  "uniqueValueInfos": [
    {
      "value": "Land Acquisition",
      "label": "Land Acquisition",
      "symbol": {
        "type": "esriSFS",
        "style": "esriSFSSolid",
        "color": [
          176,
          58,
          58,
          220
        ],
        "outline": {
          "type": "esriSLS",
          "style": "esriSLSSolid",
          "color": [
            176,
            58,
            58,
            255
          ],
          "width": 1.5
        }
      }
    },
    {
      "value": "Restoration, Stewardship, and Research",
      "label": "Restoration, Stewardship & Research",
      "symbol": {
        "type": "esriSFS",
        "style": "esriSFSSolid",
        "color": [
          240,
          145,
          12,
          220
        ],
        "outline": {
          "type": "esriSLS",
          "style": "esriSLSSolid",
          "color": [
            240,
            145,
            12,
            255
          ],
          "width": 1.5
        }
      }
    },
    {
      "value": "Both",
      "label": "Land Acquisition + Other Goals",
      "symbol": {
        "type": "esriPFS",
        "imageData": "iVBORw0KGgoAAAANSUhEUgAAABQAAAAUCAYAAACNiR0NAAAAgElEQVR4nK3TuQ2AMBQDUIeaWRggVbbIXIxBxwjMkJohGIEuUpTjn66tJzcOd4wvGEm5cGrYPLHnOmhQggHEQim2BDXYFNRiQ9CCdaAVa0APrIJeGACE79xZT+FgAPMpXCzlQoMSDCAWSrElqMGmoBYbghasA61YA3pgFfTCAOAHQMI9kNlAOf4AAAAASUVORK5CYII=",
        "contentType": "image/png",
        "width": 20,
        "height": 20,
        "angle": 0,
        "xoffset": 0,
        "yoffset": 0,
        "xscale": 1,
        "yscale": 1,
        "outline": {
          "type": "esriSLS",
          "style": "esriSLSSolid",
          "color": [
            120,
            30,
            30,
            200
          ],
          "width": 1
        }
      }
    }
  ],
  "defaultSymbol": {
    "type": "esriSFS",
    "style": "esriSFSSolid",
    "color": [
      150,
      150,
      150,
      150
    ],
    "outline": {
      "type": "esriSLS",
      "style": "esriSLSSolid",
      "color": [
        100,
        100,
        100,
        200
      ],
      "width": 0.5
    }
  },
  "defaultLabel": "Unknown / No data"
}


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


def apply_renderer(token):
    drawing_info = {"renderer": RENDERER}
    layer_def = {"drawingInfo": drawing_info}

    r = requests.post(
        f"{LAYER_URL}/updateDefinition",
        data={
            "updateDefinition": json.dumps(layer_def),
            "f": "json",
            "token": token,
        },
        timeout=60,
    )
    r.raise_for_status()
    result = r.json()
    if "error" in result:
        raise RuntimeError(f"updateDefinition error: {result['error']}")
    print("Renderer applied successfully.")
    print(json.dumps(result, indent=2))


def main():
    token = get_token()
    apply_renderer(token)


if __name__ == "__main__":
    main()
