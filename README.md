# Cinema Atlas

A multi-source data architecture project for film knowledge graphs, temporal popularity analytics, and audience behavior insights.

---

## Project Overview

Cinema Atlas models cinema as a connected analytical platform — not isolated records, but a system of relationships between movies, people, genres, production companies, audience ratings, cultural movements, and temporal popularity signals. The project builds an end-to-end pipeline from raw API and public-dataset sources through a medallion lakehouse, resolves entities across sources, derives a thematic embedding map, and surfaces everything through a Next.js web application with film profiles, analytics, and an interactive connection graph.

### Analytical questions the platform supports

- What are the strongest paths linking two films — shared crew, genre, country, or cultural movement?
- Which directors, themes, or eras act as hubs that bridge otherwise distant clusters of film?
- How did a film's box office, popularity, and audience ratings evolve over time?
- Which films are thematically nearest to a given film, regardless of genre label?
- Can relationship paths explain a recommendation, not just rank it by rating similarity?

---

## Architecture

```
TMDB API          IMDb Public Datasets        Wikidata SPARQL
  │ tmdbsimple      │ requests + gzip TSV        │ SPARQL endpoint
  ▼                 ▼                            ▼
AWS S3  ── raw JSON / JSONL, source of truth
  │        de-cinema-atlas-data/bronze/{tmdb,imdb,wikidata}/
  ▼
Databricks Volumes  ── staging .jsonl per endpoint per run
  ▼
BRONZE
  workspace.bronze.tmdb_*        (TMDB)
  workspace.bronze.imdb_*        (IMDb)
  workspace.bronze.wikidata_*    (Wikidata)
  ▼  (dedup + validate)
  *_validated / *_quarantine
  ▼  (transform + cross-source merge)
SILVER
  workspace.silver.*             (TMDB normalized star schema)
  workspace.silver.films         (TMDB ⨝ IMDb on imdb_id == tconst)
  workspace.silver.people_resolved  (entity resolution, 2-pass)
  workspace.silver.matched_tconsts
  workspace.silver.imdb_*        (IMDb Silver tables)
  workspace.silver.wikidata_*    (Wikidata Silver tables)
  workspace.silver.unified_silver (flat table — all sources merged)
  ▼  (embeddings + kNN)
GOLD
  workspace.gold.film_embeddings (UMAP 2D layout + positional color)
  workspace.gold.thematic_edges  (kNN thematic neighbor graph)
  ▼
Next.js web app (cinema_atlas_next)
```

**Single catalog:** All Bronze, Silver, and Gold tables live in `workspace` under their respective schemas. The Silver merge layer bridges TMDB and IMDb on the IMDb identifier (`tmdb.imdb_id == imdb.tconst`).

**Stack:** AWS S3 · Databricks (Unity Catalog, Delta Lake, Volumes, serverless Spark, Workflows) · TMDB API · IMDb datasets · Wikidata SPARQL · sentence-transformers · UMAP · rapidfuzz · Next.js · Cytoscape.js · Recharts

---

## Data Lineage

Lineage is traced directly from the pipeline code. Each source lands in Bronze, is validated, then flows into Silver; the merge layer joins sources on the IMDb identifier, and Gold derives the thematic map from the merged film table.

### TMDB Bronze ER Diagram
![TMDB Bronze ER](docs/images/tmdb_bronze.png)

### IMDb Bronze ER Diagram
![IMDb Bronze ER](docs/images/imdb_bronze.png)

### Wikidata Bronze ER Diagram
![Wikidata Bronze ER](docs/images/wikidata_bronze.png)

### Unified Silver ER Diagram (all sources)
![Unified Silver ER](docs/images/unified_silver.png)

---

## Data Sources

### TMDB (The Movie Database)
- **Type:** REST API, per-film JSON
- **Scope:** 2000–2026, `vote_count >= 200` (historical); all new releases (incremental)
- **Endpoints:** movie info, credits, images, releases, reviews, people
- **Role:** Primary spine — canonical titles, cast/crew, genres, metrics, box office

### IMDb
- **Type:** Public TSV.gz bulk downloads (`datasets.imdbws.com`), no API key
- **Scope:** Movies, `startYear >= 2000`; crew limited to 5 below-the-line roles
- **Files:** `title.basics`, `title.ratings`, `title.akas`, `title.principals`, `name.basics`
- **Role:** Authoritative ratings time-series, alternate titles, below-the-line crew

### Wikidata
- **Type:** SPARQL queries against `query.wikidata.org`
- **Scope:** Films (`wdt:P31 wd:Q11424`) with an IMDb ID, released ≥ 2000
- **Properties:** IMDb ID (P345), movement (P135), festival (P5072), based-on (P144), influenced-by (P737)
- **Role:** Cultural/relational context — movements, festival circuit, source material, influence links. Coverage is intentionally sparse (seed set for relationship inference, not a complete table).

---

## Pipelines
### Pipeline DAG
![Cinema Atlas Pipeline](docs/images/cinema_atlas_pipeline.png)
All pipelines run as a single Databricks Workflow (`cinema-atlas-pipeline`) with TMDB and Wikidata Bronze branches running in parallel, merging into the Silver layer once both complete.



### TMDB (catalog: `workspace`)

```
02_bronze_incremental  →  03_data_quality  →  04_silver_incremental
```

- **`02_bronze_incremental`** — discovers new films since `MAX(release_date)` (guarded against re-ingesting films already in Bronze) and refreshes metrics for films released in the last 18 months. Appends snapshots stamped with `load_ts`.
- **`03_data_quality`** — dedupes to latest snapshot per `id`, validates, writes `tmdb_*_validated` / `tmdb_*_quarantine`.
- **`04_silver_incremental`** — SCD1 MERGE for 15 dim/fact tables; SCD2 rebuild of `audience_trends`.

### IMDb (catalog: `workspace`)

```
01_bronze_ingest_historical  →  02_bronze_incremental  →  03_data_quality
```

- **`01` (one-time)** — downloads all five TSV.gz files, filters to movies ≥ 2000, writes per-film JSON (basics) and batched JSONL (others) to S3; builds the `tconst` allowlist.
- **`02` (weekly)** — `basics` MERGE on `tconst`; `ratings` APPEND as a dated snapshot (time series); `principals`/`akas`/`names` APPEND for the refresh set (last 2 years + new). Includes a one-time `RUN_MIGRATION` cell to load historical S3 files into Bronze Delta.
- **`03`** — dedup + format/range validation → `imdb_*_validated` / `imdb_*_quarantine`.

### Wikidata (catalog: `workspace`)

```
02_bronze_incremental  →  03_data_quality
```

- **`02_bronze_incremental`** — weekly incremental refresh using watermark-based MERGE for large tables (`imdb_ids`, `based_on`) and full overwrite for sparse tables (`movements`, `festivals`, `influenced_by`). Paginates SPARQL queries at 10k rows with automatic retry on timeout.
- **`03_data_quality`** — dedupes on composite keys (`wikidata_id` + property ID), validates nulls and IMDb ID format, writes `wikidata_*_validated` / `wikidata_*_quarantine`. Resolves duplicate inflation caused by Wikidata's multiple `P577` publication date entries.

Historical load was performed once using `01_bronze_ingest_historical` (Unity Catalog version), producing 5 Bronze Delta tables with 137,142 IMDb ID mappings, 7,364 festival links, 12,282 based-on relationships, 16 movement tags, and 162 influence links.

### Silver (catalog: `workspace`)

```
04_silver_films_merge  →  05_silver_people_merge  →  06_silver_wikidata  →  07_silver_imdb  →  08_unified_silver
```

- **`04_silver_films_merge`** — dedupes TMDB to latest snapshot per film, inner-joins to IMDb `basics_validated` on `imdb_id == tconst`, writes `workspace.silver.films` (10,221 matched films) and `workspace.silver.matched_tconsts`. Sanity checks enforce unique `tconst` and `id`.
- **`05_silver_people_merge`** — two-pass entity resolution:
  - **Pass 1 — direct:** TMDB `people.imdb_id → nconst` (confidence 1.0, 65,541 matches)
  - **Pass 2 — film-anchored fuzzy:** rapidfuzz `token_sort_ratio` ≥ 90 for unresolved people sharing a matched film (1,800 additional matches)
  - Writes `workspace.silver.people_resolved` with `method` and `confidence` columns
- **`06_silver_wikidata`** — joins 5 Wikidata validated tables to `matched_tconsts` on `imdb_id = tconst`, resolving to `film_id`. Produces dimension tables (`wikidata_movements`, `wikidata_festivals`) and bridge tables (`wikidata_film_movements`, `wikidata_film_festivals`, `wikidata_film_based_on`, `wikidata_film_influences`).
- **`07_silver_imdb`** — produces 4 Silver tables scoped to matched films: `imdb_film_ratings` (latest rating per film), `imdb_film_crew` (below-the-line crew), `imdb_people` (person details), `imdb_film_akas` (386,873 alternate titles by region/language).
- **`08_unified_silver`** — flat table combining all three sources into one row per film. Fields include TMDB canonical metadata, IMDb ratings and genres, Wikidata relationship arrays (`festivals`, `movements`, `based_on`, `influences`), and all three source IDs (`film_id`, `tconst`, `wikidata_id`).

### Gold (catalog: `workspace`)

- **`06_gold_thematic_embeddings`** — embeds each film's plot `overview` with `all-MiniLM-L6-v2` (384-dim), links each film to its 12 nearest thematic neighbors by cosine similarity (kNN, no hard clustering so hybrid films bridge regions), projects to 2D with UMAP, and maps position → continuous color. Writes:
  - `workspace.gold.film_embeddings` — film_id, title, year, x, y, color
  - `workspace.gold.thematic_edges` — src_film, dst_film, similarity

---

## Key Tables

### Bronze

| Catalog | Table | Type | Key |
|---|---|---|---|
| workspace | `tmdb_*_raw` | append-only | id + load_ts |
| workspace | `tmdb_*_validated` / `_quarantine` | overwrite / append | latest per id |
| workspace | `imdb_basics` | MERGE | tconst |
| workspace | `imdb_ratings` | append snapshots | tconst + snapshot_date |
| workspace | `imdb_{akas,principals,names}` | append-only | composite |
| workspace | `imdb_*_validated` / `_quarantine` | overwrite | latest per key |
| workspace | `wikidata_{imdb_ids,movements,festivals,based_on,influenced_by}` | MERGE / overwrite | wikidata_id + imdb_id |
| workspace | `wikidata_*_validated` / `_quarantine` | overwrite | latest per composite key |

### Silver

| Catalog | Table | Grain | Notes |
|---|---|---|---|
| workspace | `movies`, `people`, `film_cast`, `film_crew`, `genres`, `film_genres`, … (16 tables) | per entity | TMDB star schema; `audience_trends` is SCD2 |
| workspace | `films` | film | TMDB ⨝ IMDb merged spine (10,221 films) |
| workspace | `matched_tconsts` | tconst | join bridge (tconst ↔ film_id) |
| workspace | `people_resolved` | person | 2-pass resolution; method + confidence |
| workspace | `imdb_film_ratings` | film | latest IMDb rating + vote count |
| workspace | `imdb_film_crew` | film + person | below-the-line crew (44,197 rows) |
| workspace | `imdb_people` | person | crew people scoped to matched films |
| workspace | `imdb_film_akas` | film + region | alternate titles (386,873 rows) |
| workspace | `wikidata_movements` | movement | dimension (9 unique movements) |
| workspace | `wikidata_film_movements` | film + movement | bridge table |
| workspace | `wikidata_festivals` | festival | dimension (465 unique festivals) |
| workspace | `wikidata_film_festivals` | film + festival | bridge table (380 rows) |
| workspace | `wikidata_film_based_on` | film + source | source material links (2,135 rows) |
| workspace | `wikidata_film_influences` | film + influence | influence links (26 rows) |
| workspace | `unified_silver` | film | flat table — all sources in one row (10,221 films) |

### Gold

| Catalog | Table | Grain |
|---|---|---|
| workspace | `film_embeddings` | film (2D coords + color) |
| workspace | `thematic_edges` | film pair (kNN similarity) |

---

## unified_silver Coverage

| Metric | Count | Coverage |
|---|---|---|
| Total films | 10,221 | — |
| With Wikidata ID | 8,892 | 87% |
| With IMDb rating | 10,017 | 98% |
| With festival data | 153 | 1.5% |
| With based_on data | 2,135 | 21% |
| With movement data | 1 | <1% |
| With influence data | 12 | <1% |

---

## CDC / SCD Design

- **Bronze is append-only** — every run adds rows with a fresh `load_ts`, preserving full history.
- **`*_raw` vs `*_validated`** — raw = all snapshots; validated = latest clean row per id.
- **SCD Type 1** — most TMDB Silver tables hold current state, updated in place via MERGE.
- **SCD Type 2** — `audience_trends` (TMDB metrics) and `imdb_ratings` (IMDb ratings) preserve every timestamped snapshot for time-series analysis.

---

## Entity Resolution

The merge layer reconciles TMDB and IMDb, which use different identifiers:

- **Films:** exact join on `tmdb.imdb_id == imdb.tconst`. Inner join → only films present in both sources reach `silver.films`.
- **People:** TMDB `person_id` and IMDb `nconst` rarely share a direct key, so resolution is two-pass — direct `imdb_id` match first, then film-anchored fuzzy name matching (rapidfuzz ≥ 90) for the remainder. Output records the `method` and `confidence` so downstream consumers can filter on match quality.
- **Wikidata:** resolved via `wikidata_imdb_ids_validated.imdb_id = matched_tconsts.tconst`, mapping `wikidata_id` to `film_id`.

---

## Web Application (`cinema_atlas_next`)

Next.js app querying Databricks directly via the SQL Statement Execution REST API (`fetch()`, no Python connector — avoids the macOS SSL/Thrift issues).

**Pages:** `/` (catalog overview), `/search`, `/film/[id]` (profile + live TMDB trailer + box-office time series), `/analytics` (leaderboards + genre charts), `/graph/[id]` (Cytoscape.js connection graph — film → cast/crew → genres → related films, with person filmography expansion).

```bash
cd cinema_atlas_next
npm install
# create .env.local with Databricks + TMDB credentials
npm run dev
```

---

## Repository Structure

```
cinema-atlas/
├── cinema_atlas_next/              Next.js web application
│   ├── app/                        pages + api/ routes (server-side Databricks queries)
│   └── lib/databricks.js           Databricks REST API client
├── notebooks/
│   ├── tmdb/
│   │   ├── 02_bronze_incremental.ipynb
│   │   ├── 03_data_quality.ipynb
│   │   └── 04_silver_incremental.ipynb
│   ├── imdb/
│   │   ├── 01_bronze_ingest_historical_imdb.ipynb
│   │   ├── 02_bronze_incremental_imdb.ipynb
│   │   └── 03_data_quality_imdb.ipynb
│   ├── wikidata/
│   │   ├── 01_bronze_ingest_historical_wikidata.ipynb
│   │   ├── 02_bronze_incremental_wikidata.ipynb
│   │   └── 03_data_quality_wikidata.ipynb
│   └── silver/
│       ├── 04_silver_films_merge.ipynb
│       ├── 05_silver_people_merge.ipynb
│       ├── 06_silver_wikidata.ipynb
│       ├── 07_silver_imdb.ipynb
│       └── 08_unified_silver.ipynb
├── docs/
│   ├── images/
│   │   ├── tmdb_bronze.png
│   │   ├── imdb_bronze.png
│   │   ├── wikidata_bronze.png
│   │   └── unified_silver.png
│   ├── bronze_ingestion.md
│   ├── incremental_ingestion.md
│   └── silver_layer.md
└── README.md
```

---

## Configuration Reference

| Setting | Value |
|---|---|
| S3 bucket | `de-cinema-atlas-data` |
| Catalog | `workspace` |
| AWS region | `us-east-2` |
| TMDB refresh window | 540 days (18 months) |
| IMDb refresh window | 2 years |
| Wikidata watermark | MAX(load_ts) per table |
| Fuzzy name-match threshold | rapidfuzz token_sort_ratio ≥ 90 |
| Embedding model | `all-MiniLM-L6-v2` (384-dim) |
| kNN neighbors | 12 |

Credentials live in Databricks secret scopes (`cinema-atlas`: `aws-access-key`, `aws-secret-key` for IMDb/Wikidata; `cinema_atlas`: TMDB + AWS keys for the TMDB notebooks) and in `.env.local` for the web app.

---

## Monitoring

**TMDB — new films per run:**
```sql
WITH first_seen AS (
  SELECT id, MIN(load_ts) AS first_load
  FROM workspace.bronze.tmdb_movies_raw GROUP BY id
)
SELECT first_load, COUNT(*) AS new_films
FROM first_seen GROUP BY first_load ORDER BY first_load;
```

**Wikidata — table health check:**
```sql
SELECT 'imdb_ids' AS tbl, COUNT(*) AS rows, MAX(load_ts) AS last_load FROM workspace.bronze.wikidata_imdb_ids
UNION ALL
SELECT 'movements', COUNT(*), MAX(load_ts) FROM workspace.bronze.wikidata_movements
UNION ALL
SELECT 'festivals', COUNT(*), MAX(load_ts) FROM workspace.bronze.wikidata_festivals
UNION ALL
SELECT 'based_on', COUNT(*), MAX(load_ts) FROM workspace.bronze.wikidata_based_on
UNION ALL
SELECT 'influenced_by', COUNT(*), MAX(load_ts) FROM workspace.bronze.wikidata_influenced_by;
```

**Unified Silver — cross-source coverage:**
```sql
SELECT
    COUNT(*) AS total_films,
    COUNT(wikidata_id) AS with_wikidata,
    COUNT(imdb_rating) AS with_imdb_rating,
    SUM(CASE WHEN size(festivals) > 0 THEN 1 ELSE 0 END) AS with_festivals,
    SUM(CASE WHEN size(based_on) > 0 THEN 1 ELSE 0 END) AS with_based_on
FROM workspace.silver.unified_silver;
```

**Merge coverage — films matched across sources:**
```sql
SELECT COUNT(*) AS matched_films FROM workspace.silver.films;
```

**People resolution breakdown:**
```sql
SELECT method, COUNT(*) FROM workspace.silver.people_resolved
GROUP BY method ORDER BY 2 DESC;
```

---

## Team

Aatish Lobo · Kaio Farkouh · Tianyi Luo