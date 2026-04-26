# CLAUDE.md — `yl-hb-rgm` (RealGM scraper)

Conventions shared across the `yl-hb-*` fleet live in
[`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md) — read both.

## What this repo does

Scrapes **RealGM** for two flavors of data:

1. **Player profiles** — basketball / NBA player records (rosters,
   bios, social links) → `public.hb_talent`, `public.hb_socials`.
2. **News** — separate scrapers for NBA, NFL, soccer, hockey, baseball
   sections → `public.news`. One workflow per sport.

## Stack

**Standard enrichment** variant: TypeScript via `ts-node`, `cheerio`
(HTML parsing — no browser needed), service-role Supabase, `dotenv`.

## Repo layout

```
src/
  realgm-news-scraper.ts             # parameterised by SPORT_LABEL / NEWS_PATH
  realgm-player-scraper.ts           # NBA player profiles
  supabase.ts                        # service-role client
.github/workflows/
  realgm-player-scraper.yml
  realgm-news-scraper.yml            # NBA news (default)
  realgm-baseball-news-scraper.yml
  realgm-hockey-news-scraper.yml
  realgm-nfl-news-scraper.yml
  realgm-soccer-news-scraper.yml
package.json
tsconfig.json
```

## Supabase auth

Standard fleet convention — `SUPABASE_URL` + `SUPABASE_SERVICE_KEY`
in `src/supabase.ts`.

## Workflow lifecycle convention

Workflows call `log_workflow_run` start + result.

> Convention divergence (minor): rows for the RealGM news scrapers in
> `public.workflows` currently have `github_workflow_id = NULL` (visible
> from a sample query against the live DB). The dashboard may show the
> rows but not link runs to them until the IDs are populated. Run
> `gh api /repos/bwbcollier-byte/yl-hb-rgm/actions/workflows` and
> backfill the `github_workflow_id` column on `public.workflows` rows
> for this repo.

## Tables this repo touches

| Table | Operation | Notes |
|---|---|---|
| `public.hb_talent` | UPSERT | NBA player profiles. |
| `public.hb_socials` | UPSERT | Player social links. |
| `public.hb_companies` | UPSERT | RealGM team / franchise rows. |
| `public.hb_contacts` | UPSERT | Front-office staff where surfaced. |
| `public.news` | UPSERT | Sport-tagged articles, one workflow per sport. |
| `public.countries` | SELECT (lookup) | Country code resolution for player nationality. |

## Running locally

```bash
npm install
cp .env.example .env.local            # if present
# Set: SUPABASE_URL, SUPABASE_SERVICE_KEY, RAPIDAPI_KEY (if needed),
#      WORKFLOW_ID, BASE_URL, NEWS_PATH, SPORT_LABEL,
#      MAX_NEWS_PAGES, MAX_PAGES, MAX_PLAYERS, PLAYER_OFFSET
#      SOCIAL_TYPES (comma-separated allow-list)
npx ts-node --transpile-only src/realgm-news-scraper.ts
```

The news scraper is parameterised — each `realgm-*-news-scraper.yml`
sets a different `SPORT_LABEL` and `NEWS_PATH` and runs the same TS
file.

## Per-repo gotchas

- **Cheerio-only, no browser.** RealGM's HTML is server-rendered.
  Don't switch to Puppeteer.
- **`SOCIAL_TYPES` is an allow-list env var** (comma-separated) — used
  to filter which platforms get persisted. Tighten if specific platforms
  start producing junk.
- **`PLAYER_OFFSET`** lets you resume a partial run; don't reset it to
  0 mid-day if a long sweep was paused.
- **Per-sport workflow rows in `public.workflows` are missing
  `github_workflow_id`** — see the lifecycle note above.

## Conventions Claude should follow when editing this repo

- **One TS file per kind of scraper, parameterised by env vars.** Don't
  fork the news scraper into 5 sport-specific copies — keep it
  parameterised.

## Related repos

- `yl-hb-am`, `yl-hb-imdb`, `yl-hb-imdbp`, `yl-hb-tmdb` — sibling
  scrapers that also write to `public.news`.
- `hb_app_build` — Next.js app reading the data.
