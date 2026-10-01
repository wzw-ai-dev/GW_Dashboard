# Guided weapons: top 10 trends dashboard

Once a day, GitHub Actions collects defense-news RSS headlines, keeps the ones about guided weapons,
asks Claude (one API call) to pick, rank and summarise the top 10, and saves `docs/data.json`.
`docs/index.html` shows it. GitHub Pages hosts the page. There is no server and no manual data entry.
If any step fails, the previous `data.json` stays, so the page never goes blank (the run shows as failed in Actions).

## Edit sources
Open `config/sources.json`. Each feed is `{"name": "...", "url": "..."}`. Add, remove or fix lines.
Feeds that fail are skipped, and the Actions log names them. Check each URL opens in a browser.

## Change topic, filter, model or window
Edit `config/topic.json`: `topic` (title text and prompt), `keywords` (stories must match one),
`model`, `window_hours` (24), `widen_to_hours` (72, used when fewer than 15 stories are found),
`stale_after_hours` (36) and `categories`. Also change the `<h1>` in `docs/index.html` if you change the topic.

## Change schedule
Edit the `cron` line in `.github/workflows/update.yml`. It is in UTC: `0 5 * * *` is daily at 05:00.
You can also run it any time from the Actions tab > Update trends > Run workflow.

## Estimated cost
About 8k input and 2k output tokens per run. On Haiku 4.5 that is roughly 2 cents a run, under $1 a month
at one run a day. Check Anthropic's pricing page for current rates. GitHub Actions and Pages are free for public repos.

## Privacy
Free GitHub Pages needs a public repo, so the page and `data.json` are visible to anyone with the link.
They contain only public headlines and AI summaries. Only headlines, source names, dates, URLs and short
feed snippets are sent to Anthropic, never full articles. The API key lives only in GitHub Secrets.

## Tests
`python -m unittest discover -s tests` checks the output schema and that no invented URLs get through.
The workflow runs them before every update. No installs needed.

## Troubleshooting
- **Page says "No briefing yet":** run the workflow once from the Actions tab.
- **Run failed, "ANTHROPIC_API_KEY is not set":** add the secret (Settings > Secrets and variables > Actions).
- **Run failed, "too few candidates":** feeds are down or nothing matched. Add feeds or keywords.
- **Run failed, 401/404 from the API:** check the key, or the `model` name in `config/topic.json`.
- **Stale warning on the page:** the last run failed. Open the latest run in Actions to see why.
- **Push error in the commit step:** Settings > Actions > General > Workflow permissions > Read and write.
- **Page 404:** Settings > Pages must deploy from branch `main`, folder `/docs`.
