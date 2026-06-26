# Databricks notebook source
import tmdbsimple as tmdb
import boto3
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import functions as F
from datetime import datetime, timedelta


# CONFIG — credentials stored in Databricks secret scope "cinema_atlas"
tmdb.API_KEY = dbutils.secrets.get("cinema_atlas", "tmdb_api_key")

s3 = boto3.client(
    "s3",
    aws_access_key_id     = dbutils.secrets.get("cinema_atlas", "aws_access_key_id"),
    aws_secret_access_key = dbutils.secrets.get("cinema_atlas", "aws_secret_access_key"),
    region_name           = "us-east-2"
)
S3_BUCKET  = "de-cinema-atlas-data"
CATALOG    = "milkmoo"
VOLUME     = "/Volumes/milkmoo/bronze/tmdb_raw"
RATE_LIMIT = 0.05
MAX_CAST   = 10
MAX_WORKERS = 5
KEY_JOBS = {
    "Director", "Screenplay", "Writer", "Novel", "Characters",
    "Story", "Original Story", "Screenstory",
    "Director of Photography",
    "Original Music Composer", "Music", "Original Score", "Composer",
    "Key Makeup Artist", "Makeup Artist", "Prosthetic Makeup Artist",
    "Production Design", "Production Designer",
}

def save_to_s3(data, s3_key):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

def ingest_movie(movie_id):
    movie = tmdb.Movies(movie_id)
    save_to_s3(movie.info(),     f"bronze/tmdb/historical/movies/{movie_id}.json")
    time.sleep(RATE_LIMIT)
    save_to_s3(movie.credits(),  f"bronze/tmdb/historical/credits/{movie_id}.json")
    time.sleep(RATE_LIMIT)
    save_to_s3(movie.images(),   f"bronze/tmdb/historical/images/{movie_id}.json")
    time.sleep(RATE_LIMIT)
    save_to_s3(movie.releases(), f"bronze/tmdb/historical/releases/{movie_id}.json")
    time.sleep(RATE_LIMIT)
    save_to_s3(movie.reviews(),  f"bronze/tmdb/historical/reviews/{movie_id}.json")
    time.sleep(RATE_LIMIT)

def ingest_people(movie_id):
    movie = tmdb.Movies(movie_id)
    credits = movie.credits()
    person_ids = set()

    for member in credits.get("cast", []):
        if member.get("order", 999) <= MAX_CAST:
            person_ids.add(member.get("id"))
    for member in credits.get("crew", []):
        if member.get("job") in KEY_JOBS:
            person_ids.add(member.get("id"))

    def fetch_person(person_id):
        person = tmdb.People(person_id)
        save_to_s3(person.info(), f"bronze/tmdb/historical/people/{person_id}.json")
        time.sleep(RATE_LIMIT)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_person, pid): pid for pid in person_ids}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"  [ERROR] person: {e}")

def process_movie(args):
    i, total, movie_id = args
    try:
        ingest_movie(movie_id)
        ingest_people(movie_id)
        print(f"  [{i+1}/{total}] movie_id {movie_id} ✓")
        return movie_id
    except Exception as e:
        print(f"  [ERROR] movie_id {movie_id}: {e}")
        return None

print("Config and functions loaded")

# COMMAND ----------

from datetime import datetime

# find last release date we have in silver
cutoff = spark.sql("SELECT MAX(release_date) as max_date FROM milkmoo.silver.movies").collect()[0]["max_date"]
today = datetime.now().strftime("%Y-%m-%d")

print(f"Last film in Silver: {cutoff}")
print(f"Today: {today}")
print(f"Gap to fill: {cutoff} → {today}")

# COMMAND ----------

def discover_new_films(start_date, end_date):
    """Discover all new films released since the last silver cutoff — no vote filter."""
    print(f"\nDiscovering new films from {start_date} to {end_date}...")
    
    all_ids = []
    page = 1

    while True:
        try:
            discover = tmdb.Discover()
            response = discover.movie(
                primary_release_date_gte=str(start_date),
                primary_release_date_lte=end_date,
                sort_by="primary_release_date.asc",
                page=page
            )

            results = response.get("results", [])
            total_pages = response.get("total_pages", 1)

            if not results:
                break

            for movie in results:
                all_ids.append(movie["id"])

            print(f"  Page {page}/{total_pages} — {len(all_ids)} films found so far")

            if page >= total_pages or page >= 500:
                break

            page += 1
            time.sleep(RATE_LIMIT)

        except Exception as e:
            print(f"  [ERROR] page {page}: {e}")
            time.sleep(2)
            continue

    print(f"\nTotal new films discovered: {len(all_ids)}")
    return all_ids

# run discovery
new_film_ids = discover_new_films(cutoff, today)

# COMMAND ----------

# --- guard: drop films already in Bronze (prevents daily re-ingestion) ---
# TMDB's primary_release_date (used by discover) ≠ the release_date stored in
# movie.info(), so discovery keeps re-finding the same recent films every run.
# This filter ensures we only ingest films genuinely not yet in Bronze.

existing_ids = {
    row["id"] for row in
    spark.sql("SELECT DISTINCT id FROM milkmoo.bronze.tmdb_movies_raw").collect()
}

before = len(new_film_ids)
new_film_ids = [mid for mid in new_film_ids if mid not in existing_ids]

print(f"Discovered:        {before}")
print(f"Already in Bronze: {before - len(new_film_ids)}")
print(f"Truly new films:   {len(new_film_ids)}")

# COMMAND ----------

from datetime import datetime, timedelta

# shift cutoff by 1 day to avoid boundary overlap on future runs
cutoff_dt = datetime.strptime(str(cutoff), "%Y-%m-%d")
start_date = (cutoff_dt + timedelta(days=1)).strftime("%Y-%m-%d")

print(f"Adjusted start date: {start_date}")

# COMMAND ----------

print(f"Ingesting {len(new_film_ids)} new films...")
total = len(new_film_ids)
args = [(i, total, mid) for i, mid in enumerate(new_film_ids)]

with ThreadPoolExecutor(max_workers=3) as ex:
    futures = [ex.submit(process_movie, arg) for arg in args]
    results = [f.result() for f in as_completed(futures)]

successful = [r for r in results if r is not None]
print(f"\nDone — {len(successful)}/{total} films ingested")

# COMMAND ----------

import time
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


CATALOG = "milkmoo"
BRONZE  = "bronze"
VOLUME  = "/Volumes/milkmoo/bronze/tmdb_raw"

def _fetch_safe(key):
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.dumps(json.loads(body))
    except Exception as e:
        print("  skip", key, "-", e)
        return None

def copy_new_films(endpoint, new_ids, workers=16):
    """Only copy files for new film IDs."""
    keys = [f"bronze/tmdb/historical/{endpoint}/{mid}.json" for mid in new_ids]
    out_path = f"{VOLUME}/{endpoint}_new.jsonl"
    n = 0
    with ThreadPoolExecutor(max_workers=workers) as ex, open(out_path, "w") as f:
        for line in ex.map(_fetch_safe, keys):
            if line:
                f.write(line + "\n"); n += 1
    print(f"{endpoint}: wrote {n}/{len(keys)} -> {out_path}")
    return out_path

def copy_new_people(new_ids, workers=16):
    """Copy only people files for new film IDs by reading credits first."""
    person_ids = set()
    for mid in new_ids:
        try:
            key = f"bronze/tmdb/historical/credits/{mid}.json"
            body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
            credits = json.loads(body)
            for m in credits.get("cast", []):
                if m.get("order", 999) <= MAX_CAST:
                    person_ids.add(m.get("id"))
            for m in credits.get("crew", []):
                if m.get("job") in KEY_JOBS:
                    person_ids.add(m.get("id"))
        except Exception as e:
            print(f"  skip credits {mid}: {e}")

    print(f"  Found {len(person_ids)} unique people for new films")
    keys = [f"bronze/tmdb/historical/people/{pid}.json" for pid in person_ids]
    out_path = f"{VOLUME}/people_new.jsonl"
    n = 0
    with ThreadPoolExecutor(max_workers=workers) as ex, open(out_path, "w") as f:
        for line in ex.map(_fetch_safe, keys):
            if line:
                f.write(line + "\n"); n += 1
    print(f"people: wrote {n}/{len(keys)} -> {out_path}")
    return out_path


def read_ep_new(name, bronze_table):
    """Read new jsonl forced into the EXISTING bronze table's schema — prevents drift."""
    existing = spark.table(f"{CATALOG}.{BRONZE}.{bronze_table}")
    drop_cols = {"source_system", "source_endpoint", "source_file", "load_ts"}
    read_schema = StructType([f for f in existing.schema.fields if f.name not in drop_cols])

    return (spark.read.schema(read_schema).json(f"{VOLUME}/{name}_new.jsonl")
            .withColumn("_source_file", F.lit(f"{VOLUME}/{name}_new.jsonl")))

def add_bronze_lineage(df, endpoint):
    # movies: belongs_to_collection is stored as string in bronze, so stringify the new batch too
    if endpoint == "movies" and "belongs_to_collection" in df.columns:
        # if read_schema already typed it as string, this is a no-op; if struct, convert
        if dict(df.dtypes)["belongs_to_collection"] != "string":
            df = df.withColumn("belongs_to_collection", F.to_json(F.col("belongs_to_collection")))
    return (df.withColumn("source_system",   F.lit("tmdb"))
              .withColumn("source_endpoint", F.lit(endpoint))
              .withColumn("source_file",     F.col("_source_file"))
              .withColumn("load_ts",         F.current_timestamp())
              .drop("_source_file"))

def append_bronze(df, table):
    full = f"{CATALOG}.{BRONZE}.{table}"
    before = spark.table(full).count()
    df.write.format("delta").mode("append").saveAsTable(full)  # no mergeSchema — schema is fixed now
    after = spark.table(full).count()
    print(f"  {full}: {before:,} → {after:,} rows (+{after-before:,})")

# --- run for new films only ---
endpoints = ["movies", "credits", "releases", "reviews", "images"]

for ep in endpoints:
    t = time.time()
    copy_new_films(ep, new_film_ids)
    raw = read_ep_new(ep, f"tmdb_{ep}_raw")
    append_bronze(add_bronze_lineage(raw, ep), f"tmdb_{ep}_raw")
    print(f"   {ep} done in {time.time()-t:.0f}s\n")

# people separately
t = time.time()
copy_new_people(new_film_ids)
people_raw = read_ep_new("people", "tmdb_people_raw")
append_bronze(add_bronze_lineage(people_raw, "people"), "tmdb_people_raw")
print(f"   people done in {time.time()-t:.0f}s\n")

# COMMAND ----------

from datetime import datetime, timedelta

# refresh films released in the last 18 months — older films' box office is static
refresh_cutoff = (datetime.now() - timedelta(days=540)).strftime("%Y-%m-%d")

refresh_ids = [row["film_id"] for row in spark.sql(f"""
    SELECT film_id
    FROM milkmoo.silver.movies
    WHERE release_date >= '{refresh_cutoff}'
""").collect()]

print(f"Refresh cutoff: {refresh_cutoff}")
print(f"Films to refresh: {len(refresh_ids)}")

# COMMAND ----------

import time
from pyspark.sql import functions as F

def fetch_movie_refresh(movie_id):
    """Re-pull movie.info() — only the volatile metrics."""
    try:
        info = tmdb.Movies(movie_id).info()
        save_to_s3(info, f"bronze/tmdb/incremental/movies/{movie_id}.json")
        time.sleep(RATE_LIMIT)
        return movie_id
    except Exception as e:
        print(f"  [ERROR] movie {movie_id}: {e}")
        return None

def fetch_reviews_refresh(movie_id):
    """Re-pull reviews — new ones may have come in."""
    try:
        reviews = tmdb.Movies(movie_id).reviews()
        save_to_s3(reviews, f"bronze/tmdb/incremental/reviews/{movie_id}.json")
        time.sleep(RATE_LIMIT)
        return movie_id
    except Exception as e:
        print(f"  [ERROR] reviews {movie_id}: {e}")
        return None

# --- pull refreshed data to S3 incremental/ ---
print(f"Refreshing {len(refresh_ids)} films...")

with ThreadPoolExecutor(max_workers=5) as ex:
    list(ex.map(fetch_movie_refresh, refresh_ids))
    list(ex.map(fetch_reviews_refresh, refresh_ids))

print("S3 refresh done")

# --- copy incremental S3 files to Volume ---
def copy_incremental(endpoint, ids, workers=16):
    keys = [f"bronze/tmdb/incremental/{endpoint}/{mid}.json" for mid in ids]
    out_path = f"{VOLUME}/{endpoint}_refresh.jsonl"
    n = 0
    with ThreadPoolExecutor(max_workers=workers) as ex, open(out_path, "w") as f:
        for line in ex.map(_fetch_safe, keys):
            if line:
                f.write(line + "\n"); n += 1
    print(f"{endpoint}: wrote {n}/{len(keys)} -> {out_path}")
    return out_path

# --- append refreshed snapshots to Bronze with fresh load_ts ---
for ep in ["movies", "reviews"]:
    t = time.time()
    copy_incremental(ep, refresh_ids)
    raw = (spark.read.json(f"{VOLUME}/{ep}_refresh.jsonl")
           .withColumn("_source_file", F.col("_metadata.file_path")))
    append_bronze(add_bronze_lineage(raw, ep), f"tmdb_{ep}_raw")
    print(f"   {ep} done in {time.time()-t:.0f}s\n")

# COMMAND ----------

print("="*50)
print(f"  02_bronze_incremental — run summary")
print(f"  Run date:        {today}")
print(f"  New films added: {len(new_film_ids)}")
print(f"  Films refreshed: {len(refresh_ids)}")
print("="*50)
for ep in ["movies", "people", "credits", "releases", "reviews", "images"]:
    c = spark.table(f"milkmoo.bronze.tmdb_{ep}_raw").count()
    print(f"  bronze.tmdb_{ep}_raw: {c:,} rows")