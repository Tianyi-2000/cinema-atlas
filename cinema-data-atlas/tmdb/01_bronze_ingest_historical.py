# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration
# MAGIC Credentials are read from the Databricks secret scope `cinema_atlas`.
# MAGIC To set up: Settings → Secrets → create scope `cinema_atlas` and add keys:
# MAGIC   tmdb_api_key, aws_access_key_id, aws_secret_access_key

# COMMAND ----------

import tmdbsimple as tmdb
import boto3
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIG — credentials from Databricks secret scope
tmdb.API_KEY = dbutils.secrets.get("cinema_atlas", "tmdb_api_key")

s3 = boto3.client(
    "s3",
    aws_access_key_id     = dbutils.secrets.get("cinema_atlas", "aws_access_key_id"),
    aws_secret_access_key = dbutils.secrets.get("cinema_atlas", "aws_secret_access_key"),
    region_name           = "us-east-2"
)

S3_BUCKET        = "de-cinema-atlas-data"
MAX_CAST         = 10
MIN_VOTE_COUNT   = 200
RATE_LIMIT_DELAY = 0.05
MAX_WORKERS      = 5
KEY_JOBS = {
    "Director",
    "Screenplay", "Writer", "Novel", "Characters",
    "Story", "Original Story", "Screenstory",
    "Director of Photography",
    "Original Music Composer", "Music", "Original Score", "Composer",
    "Key Makeup Artist", "Makeup Artist", "Prosthetic Makeup Artist",
    "Production Design", "Production Designer",
}

def save_to_s3(data, s3_key):
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key,
                  Body=json.dumps(data), ContentType="application/json")

def discover_year(year):
    print(f"\nDiscovering movies for year {year}...")
    start, end, page, year_ids = f"{year}-01-01", f"{year}-12-31", 1, []
    while True:
        try:
            discover = tmdb.Discover()
            response = discover.movie(
                primary_release_date_gte=start,
                primary_release_date_lte=end,
                vote_count_gte=MIN_VOTE_COUNT,
                sort_by="primary_release_date.asc",
                page=page
            )
            results     = response.get("results", [])
            total_pages = response.get("total_pages", 1)
            if not results: break
            for movie in results:
                year_ids.append(movie["id"])
            print(f"  Page {page}/{total_pages} — {len(year_ids)} movies found so far")
            if page >= total_pages or page >= 500: break
            page += 1
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            print(f"  [ERROR] page {page}: {e}"); time.sleep(2); continue
    print(f"Year {year} — {len(year_ids)} movies discovered")
    return year_ids

def ingest_movie(movie_id):
    movie = tmdb.Movies(movie_id)
    save_to_s3(movie.info(),     f"bronze/tmdb/historical/movies/{movie_id}.json");   time.sleep(RATE_LIMIT_DELAY)
    save_to_s3(movie.credits(),  f"bronze/tmdb/historical/credits/{movie_id}.json");  time.sleep(RATE_LIMIT_DELAY)
    save_to_s3(movie.images(),   f"bronze/tmdb/historical/images/{movie_id}.json");   time.sleep(RATE_LIMIT_DELAY)
    save_to_s3(movie.releases(), f"bronze/tmdb/historical/releases/{movie_id}.json"); time.sleep(RATE_LIMIT_DELAY)
    save_to_s3(movie.reviews(),  f"bronze/tmdb/historical/reviews/{movie_id}.json");  time.sleep(RATE_LIMIT_DELAY)

def ingest_people(movie_id):
    credits    = tmdb.Movies(movie_id).credits()
    person_ids = set()
    for m in credits.get("cast", []):
        if m.get("order", 999) <= MAX_CAST: person_ids.add(m.get("id"))
    for m in credits.get("crew", []):
        if m.get("job") in KEY_JOBS: person_ids.add(m.get("id"))
    def fetch_person(pid):
        save_to_s3(tmdb.People(pid).info(), f"bronze/tmdb/historical/people/{pid}.json")
        time.sleep(RATE_LIMIT_DELAY)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_person, pid): pid for pid in person_ids}
        for f in as_completed(futures):
            try: f.result()
            except Exception as e: print(f"  [ERROR] person: {e}")

def process_movie(args):
    i, total, movie_id = args
    try:
        ingest_movie(movie_id); ingest_people(movie_id)
        print(f"  [{i+1}/{total}] movie_id {movie_id} ✓"); return movie_id
    except Exception as e:
        print(f"  [ERROR] movie_id {movie_id}: {e}"); return None

def ingest_year(year):
    movie_ids = discover_year(year)
    total     = len(movie_ids)
    print(f"\nIngesting {total} movies for year {year}...")
    args    = [(i, total, mid) for i, mid in enumerate(movie_ids)]
    with ThreadPoolExecutor(max_workers=3) as ex:
        results = [f.result() for f in as_completed([ex.submit(process_movie, a) for a in args])]
    successful = [r for r in results if r is not None]
    print(f"\nYear {year} complete — {len(successful)}/{total} movies ingested")