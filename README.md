# GBEP Interactive Conservation & Restoration Project Portal

**Client:** Galveston Bay Estuary Program (GBEP)  
**Contractor:** BCGIS  
**Live Site:** [experience.arcgis.com/experience/177ddb6fd0ce4dd7b8eebfb2938ede6d](https://experience.arcgis.com/experience/177ddb6fd0ce4dd7b8eebfb2938ede6d/)  
**Hosted Here:** [jlucas-ux.github.io/GBEP](https://jlucas-ux.github.io/GBEP/)

---

## What This Repo Is

This repository holds all the custom-built chart and display files that power the GBEP dashboard. The dashboard itself lives inside **ArcGIS Experience Builder** — ESRI's website platform — but ESRI's built-in tools couldn't match the design, data depth, or interactivity we needed. So each major section of the site is a custom HTML file we built from scratch, hosted here on GitHub Pages, and embedded into the ESRI site.

Think of it this way: ESRI is the frame on the wall, and these files are the actual art inside it.

---

## Why the Repo Has to Be Public

GitHub Pages (free hosting) only works with public repositories. This is a platform requirement, not a choice. **This does not create a security risk.** No passwords, usernames, or sensitive credentials are ever stored in this code — those live in GitHub Secrets, which are encrypted and never visible to anyone viewing the repository. See the Security section below.

---

## File Reference

### Main Dashboard Pages

| File | What It Does | Where It Lives on the Site |
|---|---|---|
| `index.html` | **Facts & Figures** — 8 interactive chart tabs including bubble chart, bar charts, treemap, heatmap, fiscal year chart, and more. Filterable by year, habitat type, and project scale. | Facts & Figures tab |
| `browse_projects.html` | **Browse Projects** — Full searchable/filterable project browser. Shows project cards, a detail panel, attachments, and lets users download data as CSV or GeoJSON. | Browse Projects tab |

### Standalone Chart & Display Widgets

These are smaller embeds — each one is a single chart or display element dropped into a specific spot on the page.

| File | What It Does |
|---|---|
| `gbep_hero_stats.html` | The four big summary numbers at the top of the page (total projects, acres, funding, etc.) — always live from the database |
| `gbep_fy_chart.html` | Funding by fiscal year — bar + line combo chart |
| `gbep_goal_chart.html` | Acres restored by project goal — donut chart |
| `gbep_species_chart.html` | Projects by focal species — custom raindrop-style infographic |
| `gbep_county_chart.html` | Projects by county — horizontal bar chart, sortable |
| `gbep_watershed_legend.html` | Color legend for the watershed map layer |
| `gbep_title_bar.html` | Page title bar — static display element |
| `gbep_about.html` | About text block with green gradient styling |

### Reference & Architecture Docs

| File | What It Does |
|---|---|
| `gbep_architecture.html` | Technical architecture diagram (for developers) |
| `gbep_architecture_simple.html` | Plain-language architecture guide (for maintainers and stakeholders) |

### Automation Scripts (Background Jobs)

These run automatically every day via GitHub Actions — no one has to trigger them manually.

| File | What It Does |
|---|---|
| `scripts/update_counties.py` | Pulls project data from Survey123, counts projects per county, and updates the GBEP Counties map layer in ArcGIS Online |
| `scripts/update_watersheds.py` | Same process for the Watershed HUC12 map layer |
| `.github/workflows/update_counties.yml` | Schedules and runs the county update script (daily at 6am UTC) |
| `.github/workflows/update_watersheds.yml` | Schedules and runs the watershed update script (daily at 6am UTC) |

---

## How the Data Flows

1. **Staff fill out a project record** using the Survey123 app on any device.
2. **Survey123 saves it** to a Feature Service (a secure database) in ArcGIS Online — automatically.
3. **When someone opens the dashboard**, each chart file fetches the latest data live from that database. No manual refresh, no export, no waiting.
4. **The county and watershed map layers** update automatically each morning via the background scripts above.

---

## How to Make Changes

**To update chart code or fix a display issue:**  
Edit the relevant `.html` file → commit to `main` → GitHub Pages deploys automatically. Changes are live within a few minutes.

**To update credentials or protected content:**  
Go to **GitHub repo → Settings → Secrets and variables → Actions**. Never put passwords or usernames directly in a file.

**To change the page layout, navigation, or map:**  
Log into ArcGIS Online → Experience Builder. No coding needed for layout changes.

---

## Security

- ✅ **No credentials in code** — all usernames, passwords, and sensitive values are stored in GitHub Secrets (encrypted, never logged, never visible in the repo)
- ✅ **Read-only public data** — the dashboard can only read project records; no one can add, edit, or delete data through the website
- ✅ **No server of our own** — everything runs in the user's browser; we don't store or log any data on our end
- ✅ **Public repo is safe** — the code being visible doesn't expose anything sensitive because sensitive values were never put in the code

---

## Maintenance Notes

- All chart files use `outFields=*` — new fields added to the Survey123 form will automatically appear in the data without any code changes required.
- The double `requestAnimationFrame` pattern used in chart initialization is intentional — ESRI's iframe container reports the wrong width on first load, and this forces a proper re-render.
- String normalization functions (`normalizeVal`, `normalizeCounty`) are used consistently across files to handle inconsistent data entry formatting.

---

## Project Context

This portal was built under an NRU grant awarded to GBEP for FY 2024. The original project proposal called for expanding GBEP's Implementation Tracking Viewer to include spatial components, summary statistics, and public-facing interactivity. This repository represents the custom development layer on top of ESRI's platform that makes that vision possible.

---

*Maintained by BCGIS · Last updated April 2026*
