# Bronze Ingestion Documentation

## 1. Purpose

This document describes the current bronze ingestion process for the Cinema Atlas project.

The bronze layer stores raw or minimally processed data from external sources before any major cleaning, normalization, or schema transformation. The purpose of this layer is to preserve the original source data so that later silver and analytics layers can be built from a reliable raw data foundation.

The current implemented bronze ingestion focuses on a partial TMDB historical movie dataset. Other sources such as IMDb, Wikipedia, Wikidata, and review datasets may be added later.

---

## 2. Current Source: TMDB Historical Movie Data

The current ingestion process uses TMDB as the source system.

### Source System

* Source: TMDB
* Data type: API-based movie metadata
* Format: JSON
* Current scope: historical movie data
* Current filter:

  * Movies released from 2000 to 2026
  * Movies with `vote_count >= 200`

This is not a full TMDB ingestion yet. It is a scoped historical ingestion used as a starting point for the project.

---

## 3. Current Ingestion Owner

The initial TMDB historical ingestion notebook was created by Kaio.

The notebook is currently used as an early bronze ingestion prototype. It pulls selected movie-related data from TMDB and writes the raw JSON responses into AWS S3.

---

## 4. Current Pipeline Flow

The current ingestion flow is:

```text
TMDB API
  → Databricks notebook
  → Python ingestion logic
  → AWS S3 bronze storage
  → raw JSON files
```

At the current stage, the notebook writes raw JSON files into S3. It does not yet create formal Databricks bronze tables.

---

## 5. Ingested TMDB Data

The current notebook ingests several types of TMDB data for each selected movie.

### Movie-level Data

* Movie information
* Movie credits
* Movie images
* Movie release information
* Movie reviews

### Person-level Data

* Person information for selected cast and crew members

The current logic also limits selected people from credits. For cast, it uses a limited number of top cast members. For crew, it focuses on selected key jobs such as directing, writing, cinematography, music, makeup, and production design.

---

## 6. Current S3 Bronze Storage Layout

The current S3 bronze layer stores TMDB data under the following folder structure:

```text
bronze/tmdb/historical/movies/
bronze/tmdb/historical/credits/
bronze/tmdb/historical/images/
bronze/tmdb/historical/releases/
bronze/tmdb/historical/reviews/
bronze/tmdb/historical/people/
```

Each file is stored as a raw JSON object.

Example pattern:

```text
bronze/tmdb/historical/movies/{movie_id}.json
bronze/tmdb/historical/credits/{movie_id}.json
bronze/tmdb/historical/images/{movie_id}.json
bronze/tmdb/historical/releases/{movie_id}.json
bronze/tmdb/historical/reviews/{movie_id}.json
bronze/tmdb/historical/people/{person_id}.json
```

This folder design separates different TMDB API response types while keeping the raw source data available for later processing.

---

## 7. Bronze Layer Interpretation

This ingestion process is considered a bronze-layer process because it preserves the raw JSON responses from TMDB with little transformation.

The bronze layer should not be responsible for final schema design, heavy cleaning, joins, or business-level aggregations. Those steps should happen later in the silver or analytics layers.

Current bronze responsibility:

```text
Collect raw TMDB data
Store raw JSON files in S3
Preserve source-level structure
Support later inspection and transformation
```

Not current bronze responsibility:

```text
Create final fact and dimension tables
Normalize nested JSON structures
Join TMDB with IMDb or Wikipedia
Build analytics-ready tables
```

---

## 8. Current Limitations and Issues

The current notebook is a good first ingestion prototype, but it is not production-clean yet.

### Security

The notebook currently contains hardcoded credentials and API keys. These should not be committed to GitHub.

Before any code is pushed to the repository, credentials should be removed and replaced with a safer approach such as:

```text
Databricks secrets
environment variables
AWS IAM roles
local .env files excluded by .gitignore
```

### Databricks Table Creation

The current notebook writes raw JSON files to S3, but it does not yet register these files as Databricks bronze tables.

Current state:

```text
TMDB API → Databricks notebook → S3 raw JSON
```

Missing future step:

```text
S3 raw JSON → Databricks bronze tables
```

### Ingestion Logging

The current process does not yet have a formal ingestion log or manifest table.

Future documentation or implementation should track:

```text
source system
endpoint name
movie_id or person_id
S3 path
ingestion timestamp
status
error message if failed
batch ID
```

### Reproducibility

The current notebook contains repeated code blocks and year-based ingestion logic. This works for exploration, but later it should be refactored into reusable functions or scripts.

---

## 9. Recommended Next Steps

The next steps for this bronze ingestion process are:

1. Remove all hardcoded credentials before committing any code.
2. Keep this documentation in GitHub under `docs/bronze_ingestion.md`.
3. Create a cleaned version of the notebook or script for the repository.
4. Add a small ingestion manifest or log design.
5. Register raw S3 JSON files as Databricks bronze tables.
6. Use the raw TMDB JSON fields to design the first silver layer schema.
7. Repeat the same bronze documentation pattern for IMDb, Wikipedia, Wikidata, and other future sources.

---

## 10. Relationship to Silver Layer

The bronze TMDB data will be used to design the first silver schema.

The silver layer should extract, clean, and normalize useful fields from the raw TMDB JSON files.

Possible silver tables may include:

```text
silver_movies
silver_people
silver_movie_credits
silver_genres
silver_movie_genres
silver_production_companies
silver_movie_companies
silver_movie_reviews
silver_movie_releases
```

The silver layer design may change as more sources are added, including IMDb, Wikipedia, Wikidata, and review datasets.

---

## 11. Summary

The current TMDB bronze ingestion process provides a useful starting point for Cinema Atlas. It collects a scoped historical movie dataset from TMDB and stores raw JSON files in S3.

However, before it becomes a stable project pipeline, the team should improve credential handling, documentation, logging, and Databricks table registration.

Current status:

```text
Good first bronze ingestion prototype.
Raw TMDB JSON is stored in S3.
Not yet production-clean.
Not yet registered as Databricks bronze tables.
Ready to support first silver schema exploration.
```
