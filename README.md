# GW University (GW Dashboard)

A Flask app for engineers who follow public defence information.
Everything is in `app.py`; Python packages are in `requirements.txt`.
Live on Render: https://gw-dashboard-slvo.onrender.com

## Apps

| Page | Route | What it does |
|---|---|---|
| Home | `/` | Dark "Welcome to GW University" page with the ASPC-GW crest and links to each app. |
| Battle Lab | `/battle-lab` | Visual map of publicly reported systems by country and category. Categories are by type of weapon (anti-ship missiles sit under Cruise missiles). Each weapon shows how that country launches it (Land / Ship / Submarine / Air launch, from public sources; table `_LAUNCH` in app.py). You can place as many strike and defence launchers as you like: land launchers inside the operating country, ships and submarines anywhere at sea (air-launched weapons cannot be placed yet), and show their published range rings; tap a launcher (or its chip) to move or remove it. It does not simulate engagements or intercepts. |
| Top 10 News | `/trends` | Four topic buttons (Hypersonic, Air & Missile Defence, Ballistic Missiles, Drones & Loitering Munitions). Each shows 10 stories as cards, each with a 3-line summary taken from the article: the 6am update's stories first, topped up with the newest stories from earlier days (up to 20 days old) when there are fewer than 10. Updates once a day at 6am only. |
| Market Research | `/research` | Finds systems of a type whose published specifications are closest to the range, speed and warhead you enter, using Wikipedia infoboxes. Also shows a "Market landscape": one chart of range and one of speed, each with every system that publishes that figure, and **Export** downloads the matches on screen as Excel (.xlsx) or, for Google Sheets, CSV (one column per system, a photo row, 12 specification rows, "Information Not Found" where the source doesn't state it). |
| Standards Library | `/standards` | Free, publicly released DoD and NASA standards, each with a link to the official copy. First cut: **Ask this standard** (a keyword search inside the document that shows the best-matching passages word for word with their PDF page) is on for MIL-STD-461 (EMI/EMC) and MIL-STD-464 (E3, including HERO); the list is `STD_ASK` in app.py. Munition & fuze safety standards are listed as reference only (no overview, no search). |

## Ground rules

- Public information only. Do not add any test, qualification, design, performance-optimisation or integration guide for weapon subsystems or weapons, and no weapon-specific technical procedures. Keep to news, trends, public contracts, public specifications of existing systems and general reference material.
- Never put secrets in the code. Set them as environment variables on Render.
- Build page text with `textContent`, not `innerHTML`.
- Keep the 6am-only update. Do not add a user refresh button.
- Free services only: NewsData.io free plan, GDELT and Wikipedia.
- Make small changes. Test each page at phone width (390px) and desktop width, with no sideways scrolling and no console errors.
- Upload changes to a feature branch, then merge into `main`.

## Environment variables (Render → Environment)

| Variable | Needed? | Purpose |
|---|---|---|
| `NEWSDATA_API_KEY` | Recommended | News search through NewsData.io (free plan). Without it, the app uses GDELT only. |
| `GH_TOKEN` | Recommended | GitHub token with *Contents: read and write* on this repo. Saves the news history and the Market Research data to a `data` branch, so they survive Render restarts. |
| `GH_REPO` | With `GH_TOKEN` | The repo, written as `owner/name`. |
| `GH_BRANCH` | Optional | Storage branch. Default `data`. Never set it to `main`, because every save would trigger a redeploy. |
| `GW_PASSWORD` / `GW_USER` | Optional | Turns on a site login. The default user name is `gw`. |
| `CRON_KEY` | Optional | Secret for `/api/daily-update?key=…`, so an outside scheduler can wake the site at 6am. |
| `GW_TZ`, `GW_UPDATE_HOUR` | Optional | Time zone and hour for the daily update. |

## Deploy

1. On GitHub, create a branch from `main`, for example `update-2026-10-27`.
2. On that branch, upload `app.py`, `requirements.txt`, `README.md` and the whole `standards_index` folder (drag the folder in) with **Add file → Upload files**, then commit. Do not upload `standards_pdfs`: the PDFs are only needed on your PC to build the search files.
3. Open a pull request into `main` and merge it. Render redeploys on its own.

Render's start command is set in the Render dashboard. It is usually `gunicorn app:app`.

## Standards search files

`standards_index/<NUMBER>.json.gz` holds the text of one standard, split into passages with section and PDF page. They are built on a PC from the official PDFs in `standards_pdfs/`. To add or update one, put the new PDF in `standards_pdfs/` (named like `MIL-STD-461.pdf`) and rebuild that file; the app picks it up on the next deploy.
