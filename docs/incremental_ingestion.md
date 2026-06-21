# Incremental Ingestion Documentation

## 1. Purpose

This document describes the **incremental** ingestion logic for the Cinema Atlas TMDB pipeline.

The historical backfill (see `bronze_ingestion.md`) loaded movies released 2000–2026 with `vote_count >= 200` as a one-time job. The incremental pipeline keeps that dataset **current** going forward, without re-loading everything each run.

It does two distinct jobs every run:

1. **Add new films** as they are released.
2. **Refresh volatile metrics** (box office, popularity, votes) and reviews for recent films.

It is implemented in the notebook `02_bronze_incremental` and runs first in the scheduled Workflow:

```text
02_bronze_incremental  →  03_data_quality  →  04_silver_incremental
```

---

## 2. Why Incremental (and not full reload)

A full re-pull of all ~13k+ films every run would be slow, waste API calls, and mostly re-fetch data that never changes (an old film's title, cast, and runtime are static).

Only two things actually change after a film is in the system:

| What changes | How often | Handled by |
|---|---|---|
| New films get released | continuously | New-films logic |
| Box office, popularity, vote counts, new reviews | while a film is recent | Metric-refresh logic |

Everything else (credits, genres, people, images, release certifications) is effectively static once captured, so it is **not** refreshed.

---

## 3. Part 1 — New Films

### Goal
Find and ingest films released since the last time the pipeline ran, so the catalog grows as new movies come out.

### Cutoff
The pipeline reads the latest release date already in Silver:

```sql
SELECT MAX(release_date) FROM milkmoo.silver.movies
```

Discovery then queries TMDB for films released from that cutoff up to today, with **no vote filter** — new releases have not accumulated votes yet, so filtering on `vote_count` would wrongly exclude films people simply haven't rated yet.

### The re-discovery problem (and the guard)

TMDB's discover endpoint filters on `primary_release_date`, but the `release_date` stored in `movie.info()` is often a *different* value (festival/limited vs general release). Because of this mismatch, discovery keeps re-finding the same recent films every run, even though they are already ingested. Left unguarded, this re-downloads and re-appends ~tens of films per day.

A guard prevents this by removing any discovered ID already present in Bronze:

```python
existing_ids = {
    row["id"] for row in
    spark.sql("SELECT DISTINCT id FROM milkmoo.bronze.tmdb_movies_raw").collect()
}
new_film_ids = [mid for mid in new_film_ids if mid not in existing_ids]
```

After the guard, only genuinely new films are ingested. This is the authoritative dedup at the **discovery** stage; `03_data_quality` still dedups again by `load_ts` as a safety net.

### What gets ingested
For each truly new film, the full set of endpoints is pulled and written to `bronze/tmdb/historical/`:
movie info, credits, images, releases, reviews, plus `people` for the top 10 cast and key crew roles. These are then appended to the Bronze Delta tables.

New films go to `historical/` (not `incremental/`) because it is the **first time** that film enters the system, regardless of when the run happens.

---

## 4. Part 2 — Metric Refresh

### Goal
Keep box office, popularity, vote counts, and reviews current for films where those numbers are still moving.

### Refresh window
Only films released in the **last 18 months** are refreshed — older films' metrics are effectively static:

```python
refresh_cutoff = (datetime.now() - timedelta(days=540)).strftime("%Y-%m-%d")
refresh_ids = spark.sql(f"""
    SELECT film_id FROM milkmoo.silver.movies
    WHERE release_date >= '{refresh_cutoff}'
""")
```

### What gets refreshed
Only the two endpoints that change over time:
- `movie.info()` — popularity, revenue, vote_count, vote_average, status
- `movie.reviews()` — new reviews may have come in

These are written to `bronze/tmdb/incremental/` and appended to Bronze with a fresh `load_ts`. Credits, images, releases, and people are **not** refreshed.

### Deleted films (404s)
Films can be removed from TMDB between discovery and refresh (duplicates, moderation). These return HTTP 404 and are skipped gracefully — they do not fail the run. They remain in Silver, so they are re-attempted (and re-404) each run; a future `is_active` flag will stop this.

---

## 5. How Incremental Data Lands in Bronze

Both parts follow the same path into Bronze:

```text
TMDB API
  → S3 (raw JSON)
  → Databricks Volume (.jsonl staging — serverless Spark cannot read s3:// directly)
  → Bronze Delta append, stamped with a fresh load_ts
```

Reads into Bronze use **schema enforcement** (`spark.read.schema(existing_bronze_schema)`) rather than schema inference. This forces each new batch into the existing table's structure and prevents `DELTA_FAILED_TO_MERGE_FIELDS` errors caused by TMDB returning slightly different nested shapes (e.g. `belongs_to_collection`, `results`) across batches.

Bronze is **append-only**: each run adds rows, never overwrites. A refreshed film therefore accumulates one row per run — this is exactly what powers the `audience_trends` time series.

---

## 6. The Role of `load_ts`

`load_ts` is the timestamp stamped on every Bronze row at append time. It is the backbone of the incremental/CDC design:

- **Two batches per run** — Part 1 (new films) and Part 2 (refresh) run minutes apart, so they receive different `load_ts` values. Grouping Bronze by `load_ts` therefore shows two rows per day (a small new-films batch and a larger refresh batch). This is expected.
- **Downstream resolution** — `03_data_quality` ranks by `load_ts` and keeps the latest snapshot per film for the SCD Type 1 Silver tables.
- **History preservation** — `04_silver_incremental` reads the full `load_ts` history from Bronze to build `audience_trends` (SCD Type 2), where each snapshot becomes a point in the metric time series and only the newest is flagged `is_current = true`.

---

## 7. Relationship to Downstream Notebooks

The incremental notebook only writes to **Bronze**. It does no cleaning, dedup, or Silver loading — those are deliberately separate:

```text
02_bronze_incremental   raw new + refreshed snapshots → bronze.*_raw
03_data_quality         dedup latest per id + validate → bronze.*_validated
04_silver_incremental   transform + MERGE → silver.* (SCD1) + audience_trends (SCD2)
```

This separation is why the Workflow runs them **in sequence with dependencies** — `03` needs `02`'s new rows, `04` needs `03`'s validated output. Running out of order would merge stale or unvalidated data.

---

## 8. Monitoring the Incremental Run

**True new films added per run** (refreshes excluded — uses earliest appearance):
```sql
WITH first_seen AS (
  SELECT id, MIN(load_ts) AS first_load
  FROM milkmoo.bronze.tmdb_movies_raw GROUP BY id
)
SELECT first_load, COUNT(*) AS new_films_added
FROM first_seen GROUP BY first_load ORDER BY first_load;
```
A healthy run shows a small new-films count each day. If a recent day is missing entirely, that day added zero genuinely new films (the guard correctly filtered repeats).

**All activity per run** (new + refresh — two rows per day expected):
```sql
SELECT load_ts, COUNT(*) AS rows
FROM milkmoo.bronze.tmdb_movies_raw
GROUP BY load_ts ORDER BY load_ts;
```

**Confirm a refreshed film's metric history:**
```sql
SELECT revenue, popularity, vote_count, snapshot_ts, is_current
FROM milkmoo.silver.audience_trends
WHERE film_id = <ID> ORDER BY snapshot_ts;
```

---

## 9. Configuration Reference

| Setting | Value |
|---|---|
| New-films cutoff | `MAX(release_date)` from `silver.movies` |
| New-films vote filter | none |
| Refresh window | 540 days (18 months) |
| Refresh endpoints | `movie.info()`, `movie.reviews()` |
| New-film S3 path | `bronze/tmdb/historical/` |
| Refresh S3 path | `bronze/tmdb/incremental/` |
| Bronze write mode | append (schema-enforced read) |
| Dedup guard | drop IDs already in `tmdb_movies_raw` |

---

## 10. Known Issues / TODO

1. **Hardcoded credentials** — move to the `cinema_atlas` Databricks secret scope and rotate the exposed keys before committing.
2. **Refresh cell read** — switch to the schema-enforced read used by the new-films cell, for consistency and drift safety.
3. **Deleted films** — add an `is_active` flag so 404'd films are not refreshed every run.
4. **Cadence** — confirm weekly vs the current daily test schedule.
5. **Manifest/log table** — record per-run counts (new films, refreshed, 404s) to a control table for auditability.