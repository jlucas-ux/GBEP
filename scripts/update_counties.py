"""
GitHub Actions script: update GBEP project counts per county
Reads from Survey123 Feature Service, writes to hosted county layer
Environment variables set via GitHub Secrets + workflow YAML
"""
import os, sys, json, urllib.request, urllib.parse, ssl

# ── Config ────────────────────────────────────────────────────────────────────
ARCGIS_USERNAME     = os.environ["ARCGIS_USERNAME"]
ARCGIS_PASSWORD     = os.environ["ARCGIS_PASSWORD"]
COUNTY_LAYER_URL  = "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/Texas_Counties_Summary_Statistics/FeatureServer/0"
COUNTY_NAME_FIELD = "CNTY_NM"

SURVEY_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "survey123_6ce0c22f05d74de6bd806994a23cbc63_results/FeatureServer/0/query"
    "?where=1%3D1"
    "&outFields=county_counties_impacted,gbep_award_amount,"
    "numeric_approximate_project_siz,nru_project_title"
    "&f=json&resultRecordCount=2000"
)

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch(url, data=None, headers=None):
    h = headers or {"Content-Type": "application/x-www-form-urlencoded"}
    req = urllib.request.Request(url, data=data, headers=h)
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return json.load(r)

# ── Step 1: Get ArcGIS token ──────────────────────────────────────────────────
print("Authenticating with ArcGIS Online...")
token_resp = fetch(
    "https://www.arcgis.com/sharing/rest/generateToken",
    data=urllib.parse.urlencode({
        "username": ARCGIS_USERNAME,
        "password": ARCGIS_PASSWORD,
        "referer":  "https://www.arcgis.com",
        "f":        "json"
    }).encode()
)
if "token" not in token_resp:
    print("ERROR: Auth failed:", token_resp)
    sys.exit(1)
token = token_resp["token"]
print("✓ Authenticated")

# ── Step 2: Fetch survey project data ─────────────────────────────────────────
print("Fetching project records from Survey123...")
features = fetch(
    SURVEY_URL,
    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://jlucas-ux.github.io/"}
).get("features", [])
print(f"✓ {len(features)} records fetched")

# ── Step 3: Tally counts per county ──────────────────────────────────────────
county_projects = {}   # name -> set of unique project titles
county_award    = {}   # name -> total GBEP award
county_acres    = {}   # name -> total acres

for f in features:
    a = f.get("attributes", {})
    raw = a.get("county_counties_impacted") or ""
    if not raw:
        continue
    title = (a.get("nru_project_title") or "").strip()
    award = a.get("gbep_award_amount") or 0
    acres = a.get("numeric_approximate_project_siz") or 0

    for part in str(raw).split(","):
        name = part.replace("_", " ").strip().title()
        if not name:
            continue
        if name not in county_projects:
            county_projects[name] = set()
            county_award[name]    = 0
            county_acres[name]    = 0
        if title and title not in county_projects[name]:
            county_projects[name].add(title)
            county_award[name] += award
            county_acres[name] += acres
        elif not title:
            county_projects[name].add(f"_untitled_{len(county_projects[name])}")

county_counts = {n: len(s) for n, s in county_projects.items()}

print("\nCounty tallies:")
for name, count in sorted(county_counts.items(), key=lambda x: -x[1]):
    print(f"  {name}: {count} projects  ${county_award[name]:,.0f}")

# ── Step 4: Fetch county features ────────────────────────────────────────────
print(f"\nFetching county polygons from hosted layer...")
layer_resp = fetch(
    COUNTY_LAYER_URL + f"/query?where=1%3D1&outFields=OBJECTID,{COUNTY_NAME_FIELD}&f=json&resultRecordCount=500&token={token}"
)
county_feats = layer_resp.get("features", [])
oid_field    = layer_resp.get("objectIdFieldName", "OBJECTID")
print(f"✓ {len(county_feats)} county features fetched")

if not county_feats:
    print("ERROR: No county features returned. Check layer URL and token.")
    sys.exit(1)

# ── Step 5: Build update list ─────────────────────────────────────────────────
def match_name(raw):
    """Try matching with and without 'County' suffix."""
    norm = str(raw).replace("_", " ").strip().title()
    if norm in county_counts:
        return norm
    # Try appending/removing "County"
    without = norm.replace(" County", "").strip()
    if without in county_counts:
        return without
    with_county = norm + " County"
    if with_county in county_counts:
        return with_county
    return None

updates = []
matched = 0

for feat in county_feats:
    attrs   = feat.get("attributes", {})
    oid     = attrs.get(oid_field)
    raw_nm  = attrs.get(COUNTY_NAME_FIELD, "")
    key     = match_name(raw_nm)
    count   = county_counts.get(key, 0) if key else 0
    award   = county_award.get(key, 0)  if key else 0
    acres   = county_acres.get(key, 0)  if key else 0
    if count > 0:
        matched += 1
    updates.append({
        "attributes": {
            oid_field:       oid,
            "project_count": count,
            "total_award":   round(award),
            "total_acres":   round(acres)
        }
    })

print(f"✓ {matched} of {len(county_feats)} counties matched to project data")

# ── Step 6: Push updates ──────────────────────────────────────────────────────
print(f"\nPushing {len(updates)} updates to hosted layer...")
resp = fetch(
    COUNTY_LAYER_URL + "/applyEdits",
    data=urllib.parse.urlencode({
        "updates": json.dumps(updates),
        "f":       "json",
        "token":   token
    }).encode()
)
results  = resp.get("updateResults", [])
success  = sum(1 for r in results if r.get("success"))
failed   = len(results) - success
print(f"✓ {success} updated successfully, {failed} failed")

for r in results:
    if not r.get("success"):
        print("  FAILED:", r)

if failed == 0:
    print("\n✓ County layer update complete!")
else:
    sys.exit(1)
