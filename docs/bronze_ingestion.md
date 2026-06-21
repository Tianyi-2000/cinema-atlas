# Bronze Ingestion Documentation

## 1. Purpose

This document describes the **bronze** ingestion process for the Cinema Atlas project.

The bronze layer stores raw or minimally processed data from external sources before any major cleaning, normalization, or schema transformation. Its purpose is to preserve the original source data so that later silver and analytics layers can be built from a reliable raw foundation.

The TMDB ingestion covers a **historical backfill** plus an **ongoing incremental pipeline**. The incremental logic (how new films and metric refreshes are pulled) is documented separately in `incremental_ingestion.md`. The silver schema and SCD design are documented in `silver_layer.md`. This file focuses on the bronze layer itself.

> **Status note:** Sections 2–8 originally described the early S3-JSON prototype. Bronze has since been extended with registered Databricks Delta tables and a data-quality layer (Sections 9–11). Where the original text described a "future step," see Section 11 for its current status.

---

## 2. Source: TMDB Movie Data

* Source: TMDB
* Data type: API-based movie metadata
* Format: JSON
* Scope: historical (2000–2026) + ongoing incremental
* Historical filter: movies released 2000–2026 with `vote_count >= 200`

The historical load is a scoped starting point. The incremental pipeline keeps it current and adds new releases over time (see `incremental_ingestion.md`).

Other sources (IMDb, Wikipedia, Wikidata, review datasets) may be added later. IMDb is in progress on a separate track.

---

## 3. Ingestion Owner

The TMDB ingestion notebooks were created by Kaio. The IMDb historical ingestion is owned by a separate teammate.

---

## 4. Pipeline Flow (bronze scope)

The original flow wrote raw JSON to S3 only:

```text
TMDB API → Databricks notebook → Python ingestion logic → AWS S3 bronze storage → raw JSON files
```

Bronze now continues into registered Delta tables:

```text
TMDB API
  → Databricks (Python / tmdbsimple)
  → AWS S3 raw JSON                      (source of truth)
  → Databricks Volume (.jsonl staging)   (serverless Spark cannot read s3:// directly)
  → Bronze Delta tables                  (registered, append-only, load_ts)
```

Everything downstream of the Bronze Delta tables (data quality, silver) is documented in the other files.

---

## 5. Ingested TMDB Data

For each selected movie the pipeline ingests:

**Movie-level:** movie info, credits, images, release information, reviews.
**Person-level:** person info for selected cast and crew.

People selection is limited: top 10 cast by billing order, and crew filtered to key creative jobs (directing, writing/screenplay, cinematography, music/composing, makeup, production design).

---

## 6. S3 Bronze Storage Layout

```text
bronze/tmdb/historical/movies/{movie_id}.json
bronze/tmdb/historical/credits/{movie_id}.json
bronze/tmdb/historical/images/{movie_id}.json
bronze/tmdb/historical/releases/{movie_id}.json
bronze/tmdb/historical/reviews/{movie_id}.json
bronze/tmdb/historical/people/{person_id}.json

bronze/tmdb/incremental/movies/{movie_id}.json     # metric refresh snapshots
bronze/tmdb/incremental/reviews/{movie_id}.json
```

`historical/` holds the first-time ingestion of any film (regardless of when the run happens). `incremental/` holds refresh snapshots of metrics and reviews for films already in the system. Each file is a raw JSON object. S3 remains the permanent source of truth.

---

## 7. Bronze Layer Interpretation

Bronze preserves raw TMDB responses with minimal transformation. It is **not** responsible for final schema design, heavy cleaning, joins, or business aggregations — those happen in silver.

Bronze responsibility: collect raw TMDB data, store it, preserve source structure, support later inspection and transformation.

---

## 8. Security

Credentials (AWS keys, TMDB API key) are currently **hardcoded** in the notebooks. Before committing to the repository they must be removed and replaced with Databricks secrets (the `cinema_atlas` scope already exists). The development keys should be rotated.

---

## 9. Bronze Delta Tables

Registered under `milkmoo.bronze`:

```text
tmdb_movies_raw
tmdb_people_raw
tmdb_credits_raw
tmdb_releases_raw
tmdb_reviews_raw
tmdb_images_raw
```

Properties:
- **Append-only** — every run adds rows; nested structures preserved (arrays/structs kept raw).
- **Lineage columns** — `source_system`, `source_endpoint`, `source_file`, `load_ts` on every row.
- **`load_ts`** is the CDC key — a refreshed film accumulates one row per run, giving full history. How this is consumed downstream is covered in `silver_layer.md`.

> **Schema enforcement on append:** new batches are read using the existing Bronze table's schema (`spark.read.schema(...)`) rather than schema inference. This prevents `DELTA_FAILED_TO_MERGE_FIELDS` errors caused by TMDB returning slightly different nested shapes (e.g. `belongs_to_collection`, `results`) across batches. `belongs_to_collection` is stored as a JSON string for the same reason.

---

## 10. Data Quality Companions

The data-quality step (`03_data_quality`, documented in `silver_layer.md` / `incremental_ingestion.md`) reads the Bronze `*_raw` tables and produces:

```text
tmdb_*_validated   # one row per id (latest load_ts), clean — input to silver
tmdb_*_quarantine  # rows that failed hard checks, with fail_reason
```

**`*_raw` vs `*_validated`:** raw is the full snapshot history (intentional duplicates across runs); validated is the latest clean row per id. These `validated` tables are the handoff point from bronze to silver.

---

## 11. Resolved "Future Steps"

The original prototype doc listed several future steps. Current status:

| Original future step | Status |
|---|---|
| Register raw S3 JSON as Databricks bronze tables | **Done** — `milkmoo.bronze.tmdb_*_raw` |
| Design the first silver schema | **Done** — see `silver_layer.md` |
| Add ingestion logging / lineage | **Partial** — lineage columns on every row; Unity Catalog tracks lineage from the Volume onward |
| Refactor repeated year-based code into reusable functions | **Done** — incremental notebook uses shared helpers |
| Remove hardcoded credentials | **Outstanding** — see Section 8 |

---

## 12. Recommended Next Steps (bronze)

1. **Remove hardcoded credentials** and rotate the exposed keys (highest priority before any commit).
2. Formalize an ingestion manifest/log table (source, endpoint, id, S3 path, timestamp, status, batch id).
3. Add an `is_active` flag for films deleted from TMDB (they 404 on every refresh otherwise).
4. Repeat this documentation pattern for IMDb, Wikipedia, Wikidata, and other future sources.

---

## 13. Related Documentation

* `incremental_ingestion.md` — how new films and metric refreshes are discovered and pulled into bronze each run.
* `silver_layer.md` — the silver schema, the data-quality step, SCD Type 1 / Type 2 design, and `audience_trends`.

---

## 14. Summary

Bronze has matured from a raw-JSON prototype into a registered Delta layer: TMDB → S3 → Volume → Bronze Delta, append-only with `load_ts` for CDC and lineage columns on every row. It feeds the data-quality step, which produces the `validated` tables consumed by silver.

```text
Historical backfill + ongoing incremental: implemented.
Bronze Delta tables registered, append-only, lineage-tagged.
Validated / quarantine companions produced by the DQ step.
Outstanding: credential hardening before repo commit.
```