# Databricks notebook source
# Configuration

import boto3
import requests
import gzip
import csv
import json
import sys
import os
from datetime import datetime, date, timezone
from delta.tables import DeltaTable
from pyspark.sql import functions as F

csv.field_size_limit(sys.maxsize)

# --- credentials (use secrets in production) ---
AWS_ACCESS_KEY = dbutils.secrets.get("cinema-atlas", "aws-access-key")
AWS_SECRET_KEY = dbutils.secrets.get("cinema-atlas", "aws-secret-key")
AWS_REGION     = "us-east-2"
S3_BUCKET      = "de-cinema-atlas-data"

# --- Unity Catalog targets (confirm these names with teammate) ---
CATALOG = "milkmoo"
SCHEMA  = "bronze"
VOLUME  = "/Volumes/milkmoo/bronze/imdb_raw"   # must exist: CREATE VOLUME milkmoo.bronze.imdb_raw

IMDB_BASE   = "https://datasets.imdbws.com"
INGEST_DATE = str(date.today())
LOAD_TS     = str(datetime.now(timezone.utc))
START_YEAR  = 2000
REFRESH_FROM_YEAR = date.today().year - 2

CREW_CATEGORIES = {
    "cinematographer", "editor", "composer",
    "production_designer", "costume_designer"
}

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

def tbl(name):
    return f"{CATALOG}.{SCHEMA}.imdb_{name}"

# download an IMDb tsv.gz and parse to a list of dicts (nulls handled)
def fetch_imdb_tsv(filename, row_filter=None, drop_cols=None):
    print(f"Downloading {filename} ...")
    resp = requests.get(f"{IMDB_BASE}/{filename}", stream=True)
    resp.raise_for_status()
    out = []
    with gzip.open(resp.raw, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            for k, v in row.items():
                if v == "\\N":
                    row[k] = None
            if row_filter and not row_filter(row):
                continue
            if drop_cols:
                for c in drop_cols:
                    row.pop(c, None)
            row["load_ts"] = LOAD_TS
            out.append(row)
    return out

# write a list of dicts to the Volume as jsonl, return the path
def rows_to_volume(rows, name):
    path = f"{VOLUME}/{name}_{INGEST_DATE}.jsonl"
    os.makedirs(VOLUME, exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"  wrote {len(rows)} rows -> {path}")
    return path

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Config ready. Catalog={CATALOG}, Volume={VOLUME}, date={INGEST_DATE}")

# COMMAND ----------

# ONE-TIME migration — set False after first successful run
RUN_MIGRATION = False

from concurrent.futures import ThreadPoolExecutor

def s3_concat_prefix_to_volume(prefix, vol_name, is_jsonl=None, workers=24):
    """Read every object under an S3 prefix via boto3, write one jsonl in the Volume.
    Auto-detects single-object vs multi-line files — is_jsonl is ignored."""
    out_path = f"{VOLUME}/migrate_{vol_name}.jsonl"

    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/"):
                keys.append(obj["Key"])

    print(f"  {prefix}: {len(keys)} files to read...")

    def fetch(key):
        try:
            return s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
        except Exception as e:
            print("  skip", key, e)
            return None

    n = 0
    with ThreadPoolExecutor(max_workers=workers) as ex, open(out_path, "w") as out:
        for body in ex.map(fetch, keys):
            if not body:
                continue
            stripped = body.strip()
            # try whole-body as a single JSON object first
            try:
                obj = json.loads(stripped)
                out.write(json.dumps(obj) + "\n"); n += 1
                continue
            except json.JSONDecodeError:
                pass
            # fall back: treat as JSONL (one object per line)
            for line in stripped.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    out.write(json.dumps(obj) + "\n"); n += 1
                except json.JSONDecodeError as e:
                    print("  bad line in", "skip:", e)

    print(f"  {prefix}: {n} rows -> {out_path}")
    return out_path

if RUN_MIGRATION:
    hist = "bronze/imdb/historical"

    # basics + ratings were per-file .json ; akas/principals/names were batched .jsonl
    sources = [
        ("basics",     f"{hist}/basics/",     False),
        ("ratings",    f"{hist}/ratings/",    False),
        ("akas",       f"{hist}/akas/",       True),
        ("principals", f"{hist}/principals/", True),
        ("names",      f"{hist}/names/",      True),
    ]

    for name, prefix, is_jsonl in sources:
        path = s3_concat_prefix_to_volume(prefix, name, is_jsonl)
        df = spark.read.json(path)
        df.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true") \
          .saveAsTable(tbl(name))
        print(f"  {tbl(name)}: {spark.table(tbl(name)).count()} rows\n")

    print("Migration complete.")
else:
    print("Migration skipped (RUN_MIGRATION = False).")

# COMMAND ----------

basics_rows = fetch_imdb_tsv(
    "title.basics.tsv.gz",
    row_filter=lambda r: r["titleType"] == "movie"
                         and r["startYear"] is not None
                         and int(r["startYear"]) >= START_YEAR
)
print(f"Parsed {len(basics_rows)} candidate movies.")

path = rows_to_volume(basics_rows, "basics")
incoming_df = spark.read.json(path)          # <-- not createDataFrame

existing = spark.table(tbl("basics")).select("tconst")   # <-- tbl(), three-level name
new_tconsts = set(
    r["tconst"] for r in
    incoming_df.join(existing, "tconst", "left_anti").select("tconst").collect()
)

DeltaTable.forName(spark, tbl("basics")).alias("t").merge(
    incoming_df.alias("s"),
    "t.tconst = s.tconst"
).whenNotMatchedInsertAll().execute()

# COMMAND ----------

movie_tconsts = set(
    r["tconst"] for r in
    spark.table("bronze.imdb_basics").select("tconst").collect()
)
print(f"Allowlist loaded from Delta: {len(movie_tconsts)} tconsts.")

# COMMAND ----------

# Job B: ratings snapshot

ratings_rows = fetch_imdb_tsv(
    "title.ratings.tsv.gz",
    row_filter=lambda r: r["tconst"] in movie_tconsts
)
for r in ratings_rows:
    r["snapshot_date"] = INGEST_DATE
    r["source"] = "imdb"
print(f"{len(ratings_rows)} rating rows for {INGEST_DATE}.")

if ratings_rows:
    path = rows_to_volume(ratings_rows, "ratings")
    incoming = spark.read.json(path)

    # align incoming to the existing table's schema (cast types, select matching cols)
    target = spark.table(tbl("ratings"))
    for field in target.schema.fields:
        if field.name in incoming.columns:
            incoming = incoming.withColumn(field.name, F.col(field.name).cast(field.dataType))

    # keep only columns the table has, in the table's order
    incoming = incoming.select([c for c in target.columns if c in incoming.columns])

    incoming.write.format("delta").mode("append").saveAsTable(tbl("ratings"))

print(f"Job B done — snapshot {INGEST_DATE} appended to {tbl('ratings')}.")

# COMMAND ----------

# build refresh set from Delta
recent = spark.table(tbl("basics")) \
    .filter(F.col("startYear").isNotNull()) \
    .filter(F.col("startYear").cast("int") >= REFRESH_FROM_YEAR) \
    .select("tconst")
refresh_tconsts = set(r["tconst"] for r in recent.collect())
refresh_tconsts.update(new_tconsts)
print(f"Refresh set: {len(refresh_tconsts)} films.")

# COMMAND ----------

# Job C-1: principals (below-the-line crew)
refresh_nconsts = set()

def _principals_filter(r):
    keep = r["tconst"] in refresh_tconsts and r["category"] in CREW_CATEGORIES
    if keep:
        refresh_nconsts.add(r["nconst"])
    return keep

crew_rows = fetch_imdb_tsv(
    "title.principals.tsv.gz",
    row_filter=_principals_filter,
    drop_cols=["characters"]
)
print(f"{len(crew_rows)} crew rows; {len(refresh_nconsts)} nconsts.")

if crew_rows:
    path = rows_to_volume(crew_rows, "principals")
    incoming = spark.read.json(path)
    target = spark.table(tbl("principals"))
    for field in target.schema.fields:
        if field.name in incoming.columns:
            incoming = incoming.withColumn(field.name, F.col(field.name).cast(field.dataType))
    incoming = incoming.select([c for c in target.columns if c in incoming.columns])
    incoming.write.format("delta").mode("append").saveAsTable(tbl("principals"))

print(f"Job C-1 done — {len(crew_rows)} crew rows appended.")

# COMMAND ----------

# Job C-2: akas
aka_rows = fetch_imdb_tsv(
    "title.akas.tsv.gz",
    row_filter=lambda r: r["titleId"] in refresh_tconsts,
    drop_cols=["attributes"]
)
print(f"{len(aka_rows)} aka rows.")

if aka_rows:
    path = rows_to_volume(aka_rows, "akas")
    incoming = spark.read.json(path)
    target = spark.table(tbl("akas"))
    for field in target.schema.fields:
        if field.name in incoming.columns:
            incoming = incoming.withColumn(field.name, F.col(field.name).cast(field.dataType))
    incoming = incoming.select([c for c in target.columns if c in incoming.columns])
    incoming.write.format("delta").mode("append").saveAsTable(tbl("akas"))

print(f"Job C-2 done — {len(aka_rows)} aka rows appended.")

# COMMAND ----------

# Job C-3: names for nconsts seen in crew refresh
if refresh_nconsts:
    name_rows = fetch_imdb_tsv(
        "name.basics.tsv.gz",
        row_filter=lambda r: r["nconst"] in refresh_nconsts
    )
    print(f"{len(name_rows)} people.")
    if name_rows:
        path = rows_to_volume(name_rows, "names")
        incoming = spark.read.json(path)
        target = spark.table(tbl("names"))
        for field in target.schema.fields:
            if field.name in incoming.columns:
                incoming = incoming.withColumn(field.name, F.col(field.name).cast(field.dataType))
        incoming = incoming.select([c for c in target.columns if c in incoming.columns])
        incoming.write.format("delta").mode("append").saveAsTable(tbl("names"))
    print(f"Job C-3 done — {len(name_rows)} people appended.")
else:
    print("No new nconsts to refresh.")

# COMMAND ----------

print(f"=== IMDb Incremental — {INGEST_DATE} ===\n")
for t in ["imdb_basics", "imdb_ratings", "imdb_akas", "imdb_principals", "imdb_names"]:
    cnt = spark.table(f"bronze.{t}").count()
    print(f"  bronze.{t:<20} {cnt:>9} rows")

print(f"\n  new films this run:   {len(new_tconsts)}")
print(f"  refresh set size:     {len(refresh_tconsts)}")