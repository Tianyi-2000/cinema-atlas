# Silver Layer Documentation — Cinema Atlas

## 1. Purpose

This document describes the Cinema Atlas **silver layer**.

The silver layer takes raw TMDB data from the bronze layer and turns it into clean, typed, deduplicated Delta tables. Nested JSON structures (genres, cast, crew, releases, reviews, etc.) are flattened and normalized into dimensions, bridges, and fact tables so the data can be queried and, later, used to build the gold analytics layer.

The silver layer was first built from the scoped historical TMDB pull (movies released 2000–2026 with `vote_count >= 200`) and is now kept current by an **incremental** process that merges new films and refreshed metrics each run. Other sources (IMDb, Wikipedia, Wikidata, MovieLens) may be conformed into silver later; the design leaves room for them but does not include them yet.

> **Status note (updated):** Sections 2–10 describe the original one-time historical build. Sections 11–15 document the incremental silver process (`04_silver_incremental`), the SCD design, and the new `audience_trends` table added since.

---

## 2. Environment and access method

The project runs on **Databricks Free Edition**, which has two constraints that shaped the design:

* Free Edition does not support Unity Catalog **external locations / storage credentials** to a user-owned S3 bucket, so the "IAM role → storage credential → external location" pattern is not available.
* Serverless compute blocks direct Spark access to `s3://` paths (no Hadoop config / instance profile).

Because of this, S3 is accessed the same way bronze ingestion works — with **boto3 and AWS access keys** — staged through a managed Volume that Spark *can* read.

> **Note:** the production target is to store these keys in the Databricks secret scope (`cinema_atlas`). The incremental notebooks currently still hardcode them; this must be remediated before committing (see Section 15).

---

## 3. Pipeline flow

### Historical build (one-time)
```text
S3 bronze (raw JSON, one file per id)
  → boto3 downloads files in parallel → one .jsonl per endpoint in a managed Volume
  → Spark reads the Volume → flatten / explode / cast / deduplicate
  → managed Delta tables in milkmoo.silver  (overwrite/append into empty tables)
```

### Incremental build (ongoing)
```text
bronze.tmdb_*_validated   (deduped + DQ-checked by 03_data_quality)
  → Spark transform (flatten / explode / cast)
  → MERGE into milkmoo.silver.*            (SCD1: update-in-place + insert-new)
  → rebuild milkmoo.silver.audience_trends (SCD2 metric history)
```

The key difference: the historical build read raw `.jsonl` and wrote into empty tables; the incremental build reads **validated Bronze Delta** and **MERGEs** into the existing tables, so it updates changed films and inserts new ones without a full reload.

The silver tables are **managed Delta tables** in the `milkmoo.silver` schema.

---

## 4. Source endpoints loaded (historical build)

| Endpoint | Files loaded |
| --- | --- |
| movies | 10,499 |
| people | 75,087 |
| credits | 10,499 |
| releases | 10,495 |
| reviews | 10,494 |

The `images` endpoint was deferred for v1.

---

## 5. Silver tables

Plain `silver.*` names are used; the **dimensional role** (dimension / fact / bridge) is documented per table.

### Dimensions

| Table | Grain | Primary key |
| --- | --- | --- |
| `silver.movies` | one film (also carries measures: budget, revenue, popularity, vote_average, vote_count) | `film_id` |
| `silver.people` | one person | `person_id` |
| `silver.genres` | one genre | `genre_id` |
| `silver.production_companies` | one company | `company_id` |
| `silver.countries` | one country | `country_iso` |
| `silver.languages` | one language | `language_iso` |

### Bridges (many-to-many)

| Table | Connects |
| --- | --- |
| `silver.film_genres` | film ↔ genre |
| `silver.film_production_companies` | film ↔ company |
| `silver.film_production_countries` | film ↔ country |
| `silver.film_spoken_languages` | film ↔ language |
| `silver.film_cast` | film ↔ person (with character, billing order) |
| `silver.film_crew` | film ↔ person (with department, job) |

### Facts (event-like)

| Table | Grain |
| --- | --- |
| `silver.film_releases` | one release per film × country × date |
| `silver.film_reviews` | one review |
| `silver.audience_trends` | one metric snapshot per film × load (SCD2 — see Section 13) |

### Supporting

| Table | Grain |
| --- | --- |
| `silver.person_aliases` | one alias per person (child of people) |

Every table also carries lineage columns: `source_system` (`'tmdb'`), `source_endpoint`, `source_file`, `loaded_at`.

---

## 6. Row counts

Historical build (baseline) and current build after several incremental runs:

| Table | Historical | Current |
| --- | --- | --- |
| movies | 10,499 | 13,366 |
| people | 75,087 | 89,510+ |
| genres | 19 | 19 |
| production_companies | 11,386 | 13,031 |
| countries | 247 | 247 |
| languages | 116 | 127 |
| film_genres | 27,068 | 30,750 |
| film_production_companies | 38,713 | 41,001 |
| film_production_countries | 16,167 | 18,060 |
| film_spoken_languages | 15,617 | 17,753 |
| film_cast | 363,329 | 377,149 |
| film_crew | 782,831 | 808,338 |
| film_releases | 301,615 | 305,987 |
| film_reviews | 16,229 | 16,278 |
| person_aliases | 67,765 | 71,963 |
| audience_trends | — | 22,805 (13,366 current) |

(Current values grow each run; treat them as a recent snapshot, not fixed.)

---

## 7. Validation results

* **Primary-key uniqueness:** 0 duplicates on `movies.film_id`, `people.person_id`, `film_cast.credit_id`, `film_crew.credit_id`, `film_reviews.review_id`.
* **Date sanity:** `release_date` within `2000-01-01` → present, matching the bronze filter and incremental additions.
* **People count:** matches loaded file counts — dedup neither dropped nor duplicated records.
* **Genre count:** 19, matching TMDB's official movie genre list.

### Cast/crew orphan rate (expected, not a defect)

A large share of `film_cast` / `film_crew` references point to a `person_id` not present in `silver.people`. This is expected: bronze only ingests full person records for the **top ~10 cast and key crew jobs** per film, so the long tail of minor cast and crew are not in `people`. The join key is valid; the gold layer should LEFT JOIN cast/crew to people.

---

## 8. Known limitations / future work

* **`images`** is deferred. The structure is designed so it can be added as a new table without changing existing ones.
* **`revenue = 0`** is common for non-US/indie films and means "unknown," not true zero — kept raw in silver, interpreted in gold.
* **Managed (not external) tables.** If the project moves to a paid Databricks workspace, switching silver to external tables under `s3://…/silver/` is a small change.

---

## 9. Relationship to gold

* `dim_film` ← `silver.movies` (descriptive columns) + surrogate key
* `dim_person` ← `silver.people`
* `dim_genre`, `dim_company`, `dim_country`, `dim_date` ← reference dimensions
* `fact_review` ← `silver.film_reviews`
* `fact_release` ← `silver.film_releases`
* `fact_film_metrics` ← **`silver.audience_trends`** (now populated — see Section 13)
* `bridge_film_cast` / `bridge_film_crew` ← silver bridges

---

## 10. Original status

The first silver build produced 15 typed, deduplicated, lineage-tagged Delta tables from the historical bronze pull. The sections below document what was added afterward to keep silver current.

---

# Updated Implementation — Incremental Silver

## 11. Incremental notebook (`04_silver_incremental`)

Runs third in the scheduled Workflow:

```text
02_bronze_incremental  →  03_data_quality  →  04_silver_incremental
```

It reads from `bronze.tmdb_*_validated` (already deduped to the latest snapshot per id and quality-checked by `03`), applies the same transform logic as the historical build, and writes into the existing silver tables via **MERGE** rather than overwrite/append. Because `03` already deduped and validated, `04` only transforms — it does not re-check or re-dedup.

---

## 12. SCD Type 1 — dimension and fact tables

All 15 original silver tables use **SCD Type 1**: matched rows are updated in place, new rows inserted. There is no history — each row reflects the current state.

```python
# simplified
target.merge(source, "t.<key> = s.<key>")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute()
```

Merge keys per table:

| Table | Merge key |
| --- | --- |
| movies | film_id |
| people | person_id |
| person_aliases | person_id + alias |
| film_cast / film_crew | credit_id |
| genres | genre_id |
| film_genres | film_id + genre_id |
| production_companies | company_id |
| film_production_companies | film_id + company_id |
| film_production_countries | film_id + country_iso |
| languages | language_iso |
| film_spoken_languages | film_id + language_iso |
| countries | country_iso |
| film_releases | film_id + country_iso + release_date + certification |
| film_reviews | review_id |

> **Null-safe keys:** `film_releases` uses null-safe equality (`<=>`) on `certification` and `release_date`, because many release rows have a null certification. Without this, SQL's `NULL != NULL` would fail to match those rows and re-insert duplicates on every run.

---

## 13. SCD Type 2 — `audience_trends`

`audience_trends` is the one table that preserves **history**. It holds a metric snapshot for each film at each load, enabling box-office / popularity / vote trends over time.

| Column | Meaning |
| --- | --- |
| `film_id` | film |
| `popularity`, `vote_average`, `vote_count`, `revenue`, `budget` | the volatile metrics |
| `snapshot_ts` | the bronze `load_ts` this snapshot came from |
| `is_current` | `true` for the latest snapshot per film, `false` for history |
| `loaded_at` | silver load timestamp |

It is rebuilt from the **full** `bronze.tmdb_movies_raw` history (not the deduped `validated` table), since it needs every snapshot. The newest snapshot per film is flagged `is_current = true`; all earlier ones `false`.

```sql
-- current value
SELECT * FROM silver.audience_trends WHERE film_id = <ID> AND is_current = true;

-- trend over time
SELECT snapshot_ts, revenue, popularity, vote_count
FROM silver.audience_trends WHERE film_id = <ID> ORDER BY snapshot_ts;
```

The volatile metrics are kept in **both** `silver.movies` (current value, SCD1) and `audience_trends` (full history, SCD2). This is deliberate — it preserves the existing `movies` schema while still enabling time-series analysis.

---

## 14. Why `raw` vs `validated` as source

| Source | Used by | Why |
| --- | --- | --- |
| `bronze.tmdb_*_validated` | SCD1 tables (movies, people, bridges, etc.) | needs current state only — one clean row per id |
| `bronze.tmdb_movies_raw` | `audience_trends` (SCD2) | needs full snapshot history for the time series |

This is the core reason both bronze layers exist: latest-state consumers read `validated`; the history consumer reads `raw`.

---

## 15. Known issues / TODO (incremental)

1. **Hardcoded credentials** in the incremental notebooks — move to the `cinema_atlas` secret scope and rotate before committing.
2. **`belongs_to_collection` is stored as a JSON string** in `movies_raw` (converted to avoid schema-merge errors across batches). The silver movies transform parses it with `get_json_object("$.id" / "$.name")`. Keep this in mind if the bronze type ever changes back.
3. **`audience_trends` full rebuild** — currently overwrites from the full bronze history each run (idempotent, cheap at current scale). If bronze history grows very large, switch to an incremental append.
4. **Change detection** — `audience_trends` currently snapshots every run even when metrics are unchanged. A future optimization is to only insert a new snapshot when a metric actually changed.

---

## 16. Summary

The silver layer is built, validated, and kept current. The historical build produced 15 typed, deduplicated, lineage-tagged Delta tables; the incremental process (`04_silver_incremental`) MERGEs new films and refreshed metrics each run, with SCD Type 1 for dimensions/facts and SCD Type 2 (`audience_trends`) for the metric time series. Primary keys are unique, dates are in range, the cast/crew orphan rate matches the bronze ingestion scope, and the layer is ready to support gold-layer dimension and fact modeling — including a real `fact_film_metrics` now that `audience_trends` provides snapshots over time.