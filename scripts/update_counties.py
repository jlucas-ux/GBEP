"""
GitHub Actions script: update GBEP project counts per county
Reads from Survey123 Feature Service, writes to hosted county layer
"""
import os, sys, json, urllib.request, urllib.parse, ssl

# ── Config from environment (GitHub Secrets) ─────────────────────────────────
AGOL_USERNAME   = os.environ["AGOL_USERNAME"]
AGOL_PASSWORD   = os.environ["AGOL_PASSWORD"]
COUNTY_LAYER_ID = os.environ.get("COUNTY_LAYER_ID", "76a4958980554c8a88db464e69e2dbbc")
COUNTY_NAME_FIELD = os.environ.get("COUNTY_NAME_FIELD", "NAME")  # set via secret or env

SURVEY_URL = (
    "https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    "survey123_6ce0c22f05d74de6bd806994a23cbc63_results/FeatureServer/0/query"
    "?where=1%3D1&outFields=county_counties_impacted,gbep_award_amount,"
    "numeric_approximate_project_siz,nru_project_title&f=json&resultRecordCount=2000"
)

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch(url, data=None, headers=None):
    headers = headers or {"Content-Type": "application/x-www-form-urlencoded"}
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return json.load(r)

# ── Step 1: Get ArcGIS token ──────────────────────────────────────────────────
print("Authenticating...")
token_data = urllib.parse.urlencode({
    "username": AGOL_USERNAME,
    "password": AGOL_PASSWORD,
    "referer": "https://www.arcgis.com",
    "f": "json"
}).encode()
token_resp = fetch(
    "https://www.arcgis.com/sharing/rest/generateToken",
    data=token_data
)
if "token" not in token_resp:
    print("Auth failed:", token_resp)
    sys.exit(1)
token = token_resp["token"]
print(f"✓ Token obtained")

# ── Step 2: Fetch survey project data ─────────────────────────────────────────
print("Fetching project data from Survey123...")
survey_resp = fetch(SURVEY_URL, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://jlucas-ux.github.io/"})
features = survey_resp.get("features", [])
print(f"✓ {len(features)} project records fetched")

# ── Step 3: Count projects per county ────────────────────────────────────────
def normalize_county(raw):
    """Normalize county name: strip underscores, title case, remove 'County' suffix for matching"""
    s = str(raw).replace("_", " ").strip().title()
    # Try both "Harris" and "Harris County" — keep full name for display
    return s

county_counts  = {}   # normalized_name -> project count
county_award   = {}   # normalized_name -> total GBEP award
county_acres   = {}   # normalized_name -> total acres
county_seen    = {}   # normalized_name -> set of project titles (dedupe)

for f in features:
    a = f.get("attributes", {})
    raw_counties = a.get("county_counties_impacted") or ""
    if not raw_counties:
        continue

    title  = (a.get("nru_project_title") or "").strip()
    award  = a.get("gbep_award_amount") or 0
    acres  = a.get("numeric_approximate_project_siz") or 0

    for c in str(raw_counties).split(","):
        name = normalize_county(c)
        if not name:
            continue
        if name not in county_counts:
            county_counts[name]  = 0
            county_award[name]   = 0
            county_acres[name]   = 0
            county_seen[name]    = set()
        # Count unique project titles per county
        if title and title not in county_seen[name]:
            county_seen[name].add(title)
            county_counts[name] += 1
            county_award[name]  += award
            county_acres[name]  += acres
        elif not title:
            county_counts[name] += 1

print("County project counts:")
for name, count in sorted(county_counts.items(), key=lambda x: -x[1]):
    print(f"  {name}: {count} projects, ${county_award[name]:,.0f} awarded")

# ── Step 4: Fetch county polygons from hosted layer ───────────────────────────
print(f"\nFetching county features from hosted layer {COUNTY_LAYER_ID}...")
layer_url = (
    f"https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    f"Texas_Counties_Summary_Statistics/FeatureServer/0/query"
    f"?where=1%3D1&outFields=*&f=json&resultRecordCount=500"
    f"&token={token}"
)
layer_resp = fetch(layer_url)
county_features = layer_resp.get("features", [])
print(f"✓ {len(county_features)} county features fetched")

if not county_features:
    print("No features returned — check layer URL and token")
    sys.exit(1)

# Detect county name field
sample_attrs = county_features[0].get("attributes", {})
name_field = COUNTY_NAME_FIELD
# Try common field name patterns if configured field not found
if name_field not in sample_attrs:
    for candidate in ["NAME", "COUNTY_NM", "CO_NAME", "CNTY_NM", "County_Name", "county_name"]:
        if candidate in sample_attrs:
            name_field = candidate
            print(f"  Using name field: {name_field}")
            break
    else:
        print("Available fields:", list(sample_attrs.keys()))
        print("ERROR: Could not find county name field. Set COUNTY_NAME_FIELD env var.")
        sys.exit(1)

oid_field = layer_resp.get("objectIdFieldName", "OBJECTID")
print(f"  Name field: {name_field}, OID field: {oid_field}")

# ── Step 5: Build update payload ─────────────────────────────────────────────
updates = []
matched = 0

for feat in county_features:
    attrs = feat.get("attributes", {})
    oid = attrs.get(oid_field)
    raw_name = str(attrs.get(name_field, ""))

    # Try matching with and without "County" suffix
    norm = normalize_county(raw_name)
    count = county_counts.get(norm, 0)
    if count == 0:
        # Try without "County" suffix
        short = norm.replace(" County", "").strip()
        count = county_counts.get(short, 0)
        award = county_award.get(short, 0)
        acres = county_acres.get(short, 0)
    else:
        award = county_award.get(norm, 0)
        acres = county_acres.get(norm, 0)

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

print(f"✓ {matched} counties matched with project data")

# ── Step 6: Push updates to ArcGIS Online ────────────────────────────────────
update_url = (
    f"https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/"
    f"Texas_Counties_Summary_Statistics/FeatureServer/0/applyEdits"
)
update_data = urllib.parse.urlencode({
    "updates": json.dumps(updates),
    "f":       "json",
    "token":   token
}).encode()

print(f"\nPushing {len(updates)} county updates...")
update_resp = fetch(update_url, data=update_data)
update_results = update_resp.get("updateResults", [])
success = sum(1 for r in update_results if r.get("success"))
failed  = len(update_results) - success
print(f"✓ {success} updated, {failed} failed")
if failed > 0:
    for r in update_results:
        if not r.get("success"):
            print("  FAILED:", r)
