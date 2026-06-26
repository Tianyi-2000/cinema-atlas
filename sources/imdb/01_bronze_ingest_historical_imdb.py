# Databricks notebook source
# DBTITLE 1,Config
import boto3
import requests
import gzip
import io
import csv
import json
from datetime import date

# AWS credentials
AWS_ACCESS_KEY = dbutils.secrets.get("cinema-atlas", "aws-access-key")
AWS_SECRET_KEY = dbutils.secrets.get("cinema-atlas", "aws-secret-key")
AWS_REGION = "us-east-2"
S3_BUCKET = "de-cinema-atlas-data"

INGEST_DATE     = str(date.today())
START_YEAR      = 2000

IMDB_BASE = "https://datasets.imdbws.com"
FILES = {
    "basics":     "title.basics.tsv.gz",
    "ratings":    "title.ratings.tsv.gz",
    "akas":       "title.akas.tsv.gz",
    "principals": "title.principals.tsv.gz",
    "names":      "name.basics.tsv.gz",
}

CREW_CATEGORIES = {
    "cinematographer",
    "editor",
    "composer",
    "production_designer",
    "costume_designer",
}

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

def save_to_s3(data, s3_key):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

print("Config loaded.")

# COMMAND ----------

# DBTITLE 1,Test S3 Connection
response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="bronze/", MaxKeys=5)
for obj in response.get("Contents", []):
    print(obj["Key"])
print("S3 connection OK.")

# COMMAND ----------

# DBTITLE 1,title.basics
# build tconst allowlist, land raw files

print("Downloading title.basics.tsv.gz ...")
resp = requests.get(f"{IMDB_BASE}/title.basics.tsv.gz", stream=True)
resp.raise_for_status()

movie_tconsts = set()
count = 0

with gzip.open(resp.raw, "rt", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        for k, v in row.items():
            if v == "\\N":
                row[k] = None
        if row["titleType"] != "movie":
            continue
        if not row["startYear"] or int(row["startYear"]) < START_YEAR:
            continue
        tconst = row["tconst"]
        movie_tconsts.add(tconst)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"bronze/imdb/historical/basics/{tconst}.json",
            Body=json.dumps(row),
            ContentType="application/json"
        )
        count += 1
        if count % 1000 == 0:
            print(f"  {count} movies written...")

print(f"\nDone — {count} movies, {len(movie_tconsts)} tconsts in allowlist.")

# COMMAND ----------

# ── CELL 3b: Rebuild movie_tconsts from S3

print("Rebuilding tconst allowlist from S3...")

paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=S3_BUCKET, Prefix="bronze/imdb/historical/basics/")

movie_tconsts = set()
for page in pages:
    for obj in page.get("Contents", []):
        # key: bronze/imdb/historical/basics/tt1234567.json
        filename = obj["Key"].split("/")[-1]          # tt1234567.json
        tconst = filename.replace(".json", "")        # tt1234567
        movie_tconsts.add(tconst)
    if len(movie_tconsts) % 50000 == 0 and len(movie_tconsts) > 0:
        print(f"  {len(movie_tconsts)} tconsts loaded...")

print(f"Done — {len(movie_tconsts)} tconsts recovered from S3.")

# COMMAND ----------

# DBTITLE 1,title.ratings
# ── CELL 4: title.ratings — batched upload ────────────────────────────────

import math

BATCH_SIZE = 10000  # rows per S3 file

print("Downloading title.ratings.tsv.gz ...")
resp = requests.get(f"{IMDB_BASE}/title.ratings.tsv.gz", stream=True)
resp.raise_for_status()

batch = []
batch_num = 0
total_rows = 0

def flush_batch(batch, batch_num):
    payload = "\n".join(json.dumps(row) for row in batch)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"bronze/imdb/historical/ratings/{INGEST_DATE}/batch_{batch_num:04d}.jsonl",
        Body=payload,
        ContentType="application/x-ndjson"
    )

with gzip.open(resp.raw, "rt", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        if row["tconst"] not in movie_tconsts:
            continue
        row["snapshot_date"] = INGEST_DATE
        row["source"] = "imdb"
        batch.append(row)
        total_rows += 1

        if len(batch) >= BATCH_SIZE:
            flush_batch(batch, batch_num)
            print(f"  batch {batch_num:04d} written ({total_rows} rows so far)")
            batch = []
            batch_num += 1

# flush remaining
if batch:
    flush_batch(batch, batch_num)
    print(f"  batch {batch_num:04d} written (final)")

print(f"\nDone — {total_rows} rating rows written in {batch_num + 1} batches.")

# COMMAND ----------

# DBTITLE 1,title.akas
# ── CELL 5: title.akas — batched ─────────────────────────────────────────

BATCH_SIZE = 10000

print("Downloading title.akas.tsv.gz ...")
resp = requests.get(f"{IMDB_BASE}/title.akas.tsv.gz", stream=True)
resp.raise_for_status()

batch = []
batch_num = 0
total_rows = 0

def flush_batch_akas(batch, batch_num):
    payload = "\n".join(json.dumps(row) for row in batch)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"bronze/imdb/historical/akas/batch_{batch_num:04d}.jsonl",
        Body=payload,
        ContentType="application/x-ndjson"
    )

with gzip.open(resp.raw, "rt", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        if row["titleId"] not in movie_tconsts:
            continue
        for k, v in row.items():
            if v == "\\N":
                row[k] = None
        row.pop("attributes", None)
        batch.append(row)
        total_rows += 1

        if len(batch) >= BATCH_SIZE:
            flush_batch_akas(batch, batch_num)
            print(f"  batch {batch_num:04d} written ({total_rows} rows so far)")
            batch = []
            batch_num += 1

if batch:
    flush_batch_akas(batch, batch_num)
    print(f"  batch {batch_num:04d} written (final)")

print(f"\nDone — {total_rows} aka rows in {batch_num + 1} batches.")

# COMMAND ----------

# ── CELL 5 (resumed): title.akas — batched, with field limit fix ──────────

import sys
import math
csv.field_size_limit(sys.maxsize)

BATCH_SIZE = 10000
SKIP_ROWS = 1420000  # rows already written — skip past these

print("Downloading title.akas.tsv.gz ...")
resp = requests.get(f"{IMDB_BASE}/title.akas.tsv.gz", stream=True)
resp.raise_for_status()

batch = []
batch_num = 142          # start numbering from next batch
total_rows = 0           # rows processed in THIS run
skipped_initial = 0      # rows skipped to resume

def flush_batch_akas(batch, batch_num):
    payload = "\n".join(json.dumps(row) for row in batch)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"bronze/imdb/historical/akas/batch_{batch_num:04d}.jsonl",
        Body=payload,
        ContentType="application/x-ndjson"
    )

with gzip.open(resp.raw, "rt", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:

        # skip rows already written
        if skipped_initial < SKIP_ROWS:
            if row["titleId"] in movie_tconsts:
                skipped_initial += 1
            continue

        if row["titleId"] not in movie_tconsts:
            continue
        for k, v in row.items():
            if v == "\\N":
                row[k] = None
        row.pop("attributes", None)
        batch.append(row)
        total_rows += 1

        if len(batch) >= BATCH_SIZE:
            flush_batch_akas(batch, batch_num)
            print(f"  batch {batch_num:04d} written ({total_rows} new rows this run)")
            batch = []
            batch_num += 1

if batch:
    flush_batch_akas(batch, batch_num)
    print(f"  batch {batch_num:04d} written (final)")

print(f"\nDone — {total_rows} additional aka rows written.")

# COMMAND ----------

# DBTITLE 1,title.principals
# CELL 6: title.principals — batched

BATCH_SIZE = 10000

print("Downloading title.principals.tsv.gz ...")
resp = requests.get(f"{IMDB_BASE}/title.principals.tsv.gz", stream=True)
resp.raise_for_status()

needed_nconsts = set()
batch = []
batch_num = 0
total_rows = 0
skipped = 0

def flush_batch_principals(batch, batch_num):
    payload = "\n".join(json.dumps(row) for row in batch)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"bronze/imdb/historical/principals/batch_{batch_num:04d}.jsonl",
        Body=payload,
        ContentType="application/x-ndjson"
    )

with gzip.open(resp.raw, "rt", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        if row["tconst"] not in movie_tconsts:
            continue
        if row["category"] not in CREW_CATEGORIES:
            skipped += 1
            continue
        for k, v in row.items():
            if v == "\\N":
                row[k] = None
        row.pop("characters", None)
        needed_nconsts.add(row["nconst"])
        batch.append(row)
        total_rows += 1

        if len(batch) >= BATCH_SIZE:
            flush_batch_principals(batch, batch_num)
            print(f"  batch {batch_num:04d} written ({total_rows} rows so far)")
            batch = []
            batch_num += 1

if batch:
    flush_batch_principals(batch, batch_num)
    print(f"  batch {batch_num:04d} written (final)")

print(f"\nDone — {total_rows} crew rows in {batch_num + 1} batches, {skipped} skipped.")
print(f"{len(needed_nconsts)} unique nconsts to fetch.")

# save needed_nconsts to S3 so session loss doesn't kill Cell 7
s3.put_object(
    Bucket=S3_BUCKET,
    Key="bronze/imdb/historical/meta/needed_nconsts.json",
    Body=json.dumps(list(needed_nconsts)),
    ContentType="application/json"
)
print("nconsts saved to S3.")

# COMMAND ----------

# DBTITLE 1,name.basics
# CELL 7: name.basics — batched

# recovery line in case of session loss
if not needed_nconsts:
    body = s3.get_object(
        Bucket=S3_BUCKET,
        Key="bronze/imdb/historical/meta/needed_nconsts.json"
    )["Body"].read()
    needed_nconsts = set(json.loads(body))
    print(f"Recovered {len(needed_nconsts)} nconsts from S3.")

BATCH_SIZE = 10000

print("Downloading name.basics.tsv.gz ...")
resp = requests.get(f"{IMDB_BASE}/name.basics.tsv.gz", stream=True)
resp.raise_for_status()

batch = []
batch_num = 0
total_rows = 0

def flush_batch_names(batch, batch_num):
    payload = "\n".join(json.dumps(row) for row in batch)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"bronze/imdb/historical/names/batch_{batch_num:04d}.jsonl",
        Body=payload,
        ContentType="application/x-ndjson"
    )

with gzip.open(resp.raw, "rt", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        if row["nconst"] not in needed_nconsts:
            continue
        for k, v in row.items():
            if v == "\\N":
                row[k] = None
        batch.append(row)
        total_rows += 1

        if len(batch) >= BATCH_SIZE:
            flush_batch_names(batch, batch_num)
            print(f"  batch {batch_num:04d} written ({total_rows} rows so far)")
            batch = []
            batch_num += 1

if batch:
    flush_batch_names(batch, batch_num)
    print(f"  batch {batch_num:04d} written (final)")

print(f"\nDone — {total_rows} people written in {batch_num + 1} batches.")

# COMMAND ----------

# Summary

prefixes = [
    "bronze/imdb/historical/basics/",
    f"bronze/imdb/historical/ratings/{INGEST_DATE}/",
    "bronze/imdb/historical/akas/",
    "bronze/imdb/historical/principals/",
    "bronze/imdb/historical/names/",
]

print("\n=== IMDb Historical Ingestion Summary ===")
for prefix in prefixes:
    paginator = s3.get_paginator("list_objects_v2")
    count = sum(
        page.get("KeyCount", 0)
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
    )
    print(f"  {prefix:<55} {count:>7} files")