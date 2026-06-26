# Databricks notebook source
import requests

ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host  = ctx.apiUrl().get()
token = ctx.apiToken().get()
H = {"Authorization": f"Bearer {token}"}
SCOPE = "cinema_atlas"

# create the locked box (safe to run more than once)
r = requests.post(f"{host}/api/2.0/secrets/scopes/create", headers=H, json={"scope": SCOPE})
print("scope create:", r.status_code, r.text or "(created)")

# show two input boxes at the very top of the notebook
print("Paste your keys into the two boxes that appeared at the top of the notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC # testing, will delete later

# COMMAND ----------

import requests
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
H = {"Authorization": f"Bearer {token}"}
SCOPE = "cinema_atlas"


for k, v in [("aws_access_key_id", ACCESS_KEY_ID), ("aws_secret_access_key", SECRET_ACCESS_KEY)]:
    rr = requests.post(f"{host}/api/2.0/secrets/put", headers=H,
                       json={"scope": SCOPE, "key": k, "string_value": v})
    print("stored", k, "->", rr.status_code)

print("check:", dbutils.secrets.list(SCOPE))

# COMMAND ----------

dbutils.widgets.removeAll()
print(dbutils.secrets.list(SCOPE))   # shows the key NAMES only, never the values

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Connect to S3 + access test
# MAGIC Builds the boto3 client using the AWS keys stored in the `cinema_atlas` secret box,
# MAGIC then lists a few files in the bronze `people/` folder to confirm Databricks can read S3.

# COMMAND ----------

import boto3

SCOPE      = "cinema_atlas"
AWS_REGION = "us-east-2"
S3_BUCKET  = "de-cinema-atlas-data"

# build the S3 client using keys pulled from the secret box (not hardcoded)
s3 = boto3.client(
    "s3",
    aws_access_key_id     = dbutils.secrets.get(SCOPE, "aws_access_key_id"),
    aws_secret_access_key = dbutils.secrets.get(SCOPE, "aws_secret_access_key"),
    region_name = AWS_REGION,
)

# list the first few files under the people folder to prove access
resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="bronze/tmdb/historical/people/", MaxKeys=5)
for obj in resp.get("Contents", []):
    print(obj["Key"], "-", obj["Size"], "bytes")
print("KeyCount:", resp.get("KeyCount"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Debug: check the stored key shapes
# MAGIC Prints only the lengths and prefix so we can tell if the key ID and secret got swapped or mistyped. Never shows the actual values.

# COMMAND ----------

kid = dbutils.secrets.get("cinema_atlas", "aws_access_key_id")
sec = dbutils.secrets.get("cinema_atlas", "aws_secret_access_key")
print("access_key_id -> length:", len(kid), "| starts with 'AKIA':", kid.startswith("AKIA"))
print("secret_key    -> length:", len(sec))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read one bronze JSON file (content check)
# MAGIC Downloads a single people file from S3 and parses it, to confirm we can read actual file contents — not just list filenames.

# COMMAND ----------

# === 2. Read one bronze JSON file (content check) ===
# Pulls one people file from S3 and parses the JSON so we can see the fields.

import json

obj  = s3.get_object(Bucket=S3_BUCKET, Key="bronze/tmdb/historical/people/1.json")
data = json.loads(obj["Body"].read())

print("name:", data.get("name"))
print("fields:", list(data.keys()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Count bronze files per endpoint
# MAGIC Confirms which bronze folders actually have data before we build silver.

# COMMAND ----------

# === 3. Count bronze files per endpoint ===
# Counts JSON files in each bronze folder so we know what's safe to build from.

folders = ["movies", "people", "credits", "releases", "reviews", "images"]
paginator = s3.get_paginator("list_objects_v2")
for f in folders:
    prefix = f"bronze/tmdb/historical/{f}/"
    total = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        total += page.get("KeyCount", 0)
    print(f"{f:10s}: {total} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Create a managed Volume for raw bronze files
# MAGIC Serverless Spark can't read s3:// directly, so we copy bronze JSON into this Databricks-managed Volume, then Spark reads from here.

# COMMAND ----------

# === 4. Create a managed Volume to hold raw bronze files ===
spark.sql("CREATE VOLUME IF NOT EXISTS milkmoo.bronze.tmdb_raw")
print("Volume ready: /Volumes/milkmoo/bronze/tmdb_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Helper: copy a bronze endpoint from S3 into the Volume
# MAGIC Lists an endpoint's files, downloads them in parallel, and writes them as one .jsonl file (one JSON record per line) in the Volume for Spark to read.

# COMMAND ----------

# === 5. Helper: copy a bronze endpoint from S3 -> Volume as one .jsonl ===
import json
from concurrent.futures import ThreadPoolExecutor

VOLUME = "/Volumes/milkmoo/bronze/tmdb_raw"
paginator = s3.get_paginator("list_objects_v2")

def list_keys(endpoint):
    prefix = f"bronze/tmdb/historical/{endpoint}/"
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys

def _fetch(key):
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.dumps(json.loads(body))   # compact: one line per record

def copy_endpoint(endpoint, limit=None, workers=16):
    keys = list_keys(endpoint)
    if limit:
        keys = keys[:limit]
    out_path = f"{VOLUME}/{endpoint}.jsonl"
    with ThreadPoolExecutor(max_workers=workers) as ex, open(out_path, "w") as f:
        for line in ex.map(_fetch, keys):
            f.write(line + "\n")
    print(f"{endpoint}: copied {len(keys)} files -> {out_path}")
    return out_path

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Test the copy on a small sample
# MAGIC Copies just 200 people files and reads them into Spark, to confirm the whole path works before the full load.

# COMMAND ----------

# === 6. Small test: 200 people files -> Volume -> Spark ===
path = copy_endpoint("people", limit=200)
df = spark.read.json(path)
print("rows:", df.count())
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Full load: copy all core endpoints into the Volume
# MAGIC Downloads every file for the five core endpoints into the Volume as .jsonl. People (75k) is the slow one — expect several minutes total. Skips any rare unreadable file instead of failing.

# COMMAND ----------

# === 7. Full load: all core endpoints -> Volume ===
import time

# fault-tolerant fetch: skip a rare bad file instead of failing the whole job
def _fetch_safe(key):
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.dumps(json.loads(body))
    except Exception as e:
        print("  skip", key, "-", e)
        return None

def copy_endpoint_safe(endpoint, workers=16):
    keys = list_keys(endpoint)
    out_path = f"{VOLUME}/{endpoint}.jsonl"
    n = 0
    with ThreadPoolExecutor(max_workers=workers) as ex, open(out_path, "w") as f:
        for line in ex.map(_fetch_safe, keys):
            if line:
                f.write(line + "\n"); n += 1
    print(f"{endpoint}: wrote {n}/{len(keys)} -> {out_path}")

for ep in ["movies", "people", "credits", "releases", "reviews"]:
    t = time.time()
    copy_endpoint_safe(ep)
    print(f"   took {time.time()-t:.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Silver setup: read Volume files + helpers
# MAGIC Reads the five .jsonl files into Spark and defines small helpers: lineage columns, a date parser, and a function that writes a managed Delta table into milkmoo.silver.

# COMMAND ----------

# === 8. Silver setup: read Volume + helpers ===
from pyspark.sql import functions as F

CATALOG, SCHEMA = "milkmoo", "silver"
VOLUME = "/Volumes/milkmoo/bronze/tmdb_raw"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

def read_ep(name):
    # UC-friendly source-file lineage via _metadata.file_path (not input_file_name)
    return spark.read.json(f"{VOLUME}/{name}.jsonl").withColumn("_source_file", F.col("_metadata.file_path"))

def add_lineage(df, endpoint):
    out = (df.withColumn("source_system", F.lit("tmdb"))
             .withColumn("source_endpoint", F.lit(endpoint))
             .withColumn("source_file", F.col("_source_file") if "_source_file" in df.columns else F.lit(None).cast("string"))
             .withColumn("loaded_at", F.current_timestamp()))
    return out.drop("_source_file") if "_source_file" in out.columns else out

def write_silver(df, table):
    full = f"{CATALOG}.{SCHEMA}.{table}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(full)
    print(f"  {full}: {spark.table(full).count():,} rows")

def to_date10(c):     return F.to_date(F.substring(c,1,10),"yyyy-MM-dd")
def empty_to_null(c): return F.when(F.trim(c)=="", None).otherwise(c)

movies_raw   = read_ep("movies")
people_raw   = read_ep("people")
credits_raw  = read_ep("credits")
releases_raw = read_ep("releases")
reviews_raw  = read_ep("reviews")
print("raw loaded:", movies_raw.count(), "movies")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Build silver.movies (dimension)
# MAGIC One clean, typed row per film: ids, title, dates, runtime, budget/revenue, vote metrics, collection, plus lineage columns.

# COMMAND ----------

# === 9. silver.movies (dimension) ===
movies = (movies_raw.select(
    F.col("id").cast("bigint").alias("film_id"),
    empty_to_null(F.col("imdb_id")).alias("imdb_id"),
    F.col("title"), F.col("original_title"), F.col("original_language"), F.col("status"),
    to_date10(F.col("release_date")).alias("release_date"),
    F.col("runtime").cast("int").alias("runtime"),
    F.col("budget").cast("bigint").alias("budget"),
    F.col("revenue").cast("bigint").alias("revenue"),
    F.col("popularity").cast("double").alias("popularity"),
    F.col("vote_average").cast("double").alias("vote_average"),
    F.col("vote_count").cast("int").alias("vote_count"),
    F.col("adult").cast("boolean").alias("adult"),
    F.col("video").cast("boolean").alias("video"),
    empty_to_null(F.col("homepage")).alias("homepage"),
    empty_to_null(F.col("tagline")).alias("tagline"),
    F.col("overview"),
    F.col("belongs_to_collection.id").cast("bigint").alias("collection_id"),
    F.col("belongs_to_collection.name").alias("collection_name"),
    F.col("poster_path"), F.col("backdrop_path"),
    F.col("_source_file"),
).dropDuplicates(["film_id"]))

write_silver(add_lineage(movies, "movies"), "movies")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Movie-derived dimensions + [bridges](url)

# COMMAND ----------

# === 10. genres, companies, countries, languages (+ bridges) ===
gx = movies_raw.select(F.col("id").cast("bigint").alias("film_id"), F.explode("genres").alias("g"))
genres = gx.select(F.col("g.id").cast("int").alias("genre_id"), F.col("g.name").alias("genre_name")).dropDuplicates(["genre_id"])
film_genres = gx.select("film_id", F.col("g.id").cast("int").alias("genre_id")).dropDuplicates(["film_id", "genre_id"])
write_silver(add_lineage(genres, "movies"), "genres")
write_silver(add_lineage(film_genres, "movies"), "film_genres")
pcx = movies_raw.select(F.col("id").cast("bigint").alias("film_id"), F.explode("production_companies").alias("c"))
companies = pcx.select(F.col("c.id").cast("bigint").alias("company_id"), F.col("c.name").alias("company_name"), empty_to_null(F.col("c.origin_country")).alias("origin_country"), F.col("c.logo_path").alias("logo_path")).dropDuplicates(["company_id"])
film_companies = pcx.select("film_id", F.col("c.id").cast("bigint").alias("company_id")).dropDuplicates(["film_id", "company_id"])
write_silver(add_lineage(companies, "movies"), "production_companies")
write_silver(add_lineage(film_companies, "movies"), "film_production_companies")
pcox = movies_raw.select(F.col("id").cast("bigint").alias("film_id"), F.explode("production_countries").alias("c"))
film_countries = pcox.select("film_id", F.col("c.iso_3166_1").alias("country_iso")).dropDuplicates(["film_id", "country_iso"])
write_silver(add_lineage(film_countries, "movies"), "film_production_countries")
lx = movies_raw.select(F.col("id").cast("bigint").alias("film_id"), F.explode("spoken_languages").alias("l"))
languages = lx.select(F.col("l.iso_639_1").alias("language_iso"), F.col("l.name").alias("language_name"), F.col("l.english_name").alias("english_name")).dropDuplicates(["language_iso"])
film_languages = lx.select("film_id", F.col("l.iso_639_1").alias("language_iso")).dropDuplicates(["film_id", "language_iso"])
write_silver(add_lineage(languages, "movies"), "languages")
write_silver(add_lineage(film_languages, "movies"), "film_spoken_languages")
prod_c = movies_raw.select(F.explode("production_countries").alias("c")).select(F.col("c.iso_3166_1").alias("country_iso"), F.col("c.name").alias("country_name"))
rel_c = releases_raw.select(F.explode("countries").alias("c")).select(F.col("c.iso_3166_1").alias("country_iso"), F.lit(None).cast("string").alias("country_name"))
countries = prod_c.unionByName(rel_c).groupBy("country_iso").agg(F.max("country_name").alias("country_name")).where(F.col("country_iso").isNotNull())
write_silver(add_lineage(countries, "movies"), "countries")

# COMMAND ----------

# MAGIC %md
# MAGIC 11. silver.people (dimension) + person_aliases

# COMMAND ----------

# DBTITLE 1,Cell 28
# === 11. people + person_aliases ===
people = (people_raw.select(
    F.col("id").cast("bigint").alias("person_id"),
    empty_to_null(F.col("imdb_id")).alias("imdb_id"),
    F.col("name"), F.col("gender").cast("int").alias("gender"),
    to_date10(F.col("birthday")).alias("birthday"),
    to_date10(F.col("deathday")).alias("deathday"),
    F.col("known_for_department"),
    empty_to_null(F.col("place_of_birth")).alias("place_of_birth"),
    F.col("popularity").cast("double").alias("popularity"),
    F.col("biography"), F.col("profile_path"), F.col("_source_file"),
))
write_silver(add_lineage(people, "people"), "people")

aliases = (people_raw.select(F.col("id").cast("bigint").alias("person_id"),
           F.explode_outer("also_known_as").alias("alias"), F.col("_source_file"))
           .where(F.col("alias").isNotNull() & (F.trim(F.col("alias"))!=""))
           .dropDuplicates(["person_id","alias"]))
write_silver(add_lineage(aliases, "people"), "person_aliases")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### 12. Bridges: film_cast + film_crew

# COMMAND ----------

# === 12. film_cast + film_crew (from credits) ===
cast = (credits_raw.select(F.col("id").cast("bigint").alias("film_id"),
        F.explode("cast").alias("c"), F.col("_source_file"))
        .select(F.col("c.credit_id").alias("credit_id"), "film_id",
                F.col("c.id").cast("bigint").alias("person_id"),
                F.col("c.character").alias("character"),
                F.col("c.order").cast("int").alias("cast_order"),
                F.col("c.cast_id").cast("int").alias("cast_id"), "_source_file")
        .dropDuplicates(["credit_id"]))
write_silver(add_lineage(cast, "credits"), "film_cast")

crew = (credits_raw.select(F.col("id").cast("bigint").alias("film_id"),
        F.explode("crew").alias("c"), F.col("_source_file"))
        .select(F.col("c.credit_id").alias("credit_id"), "film_id",
                F.col("c.id").cast("bigint").alias("person_id"),
                F.col("c.department").alias("department"),
                F.col("c.job").alias("job"), "_source_file")
        .dropDuplicates(["credit_id"]))
write_silver(add_lineage(crew, "credits"), "film_crew")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. Facts: film_releases + film_reviews

# COMMAND ----------

# === 13. film_releases + film_reviews ===
rel = (releases_raw.select(F.col("id").cast("bigint").alias("film_id"),
       F.explode("countries").alias("c"), F.col("_source_file"))
       .select("film_id", F.col("c.iso_3166_1").alias("country_iso"),
               empty_to_null(F.col("c.certification")).alias("certification"),
               to_date10(F.col("c.release_date")).alias("release_date"),
               F.col("c.primary").cast("boolean").alias("is_primary"),
               F.lit(None).cast("int").alias("release_type"),
               F.col("c.descriptors").alias("descriptors"), "_source_file")
       .dropDuplicates(["film_id","country_iso","release_date","certification"]))
write_silver(add_lineage(rel, "releases"), "film_releases")

rev = (reviews_raw.select(F.col("id").cast("bigint").alias("film_id"),
       F.explode("results").alias("r"), F.col("_source_file"))
       .select(F.col("r.id").alias("review_id"), "film_id",
               F.col("r.author").alias("author"),
               F.col("r.author_details.username").alias("author_username"),
               F.col("r.author_details.rating").cast("double").alias("author_rating"),
               F.col("r.content").alias("content"),
               F.to_timestamp(F.col("r.created_at")).alias("created_at"),
               F.to_timestamp(F.col("r.updated_at")).alias("updated_at"),
               F.col("r.url").alias("url"), "_source_file")
       .dropDuplicates(["review_id"]))
write_silver(add_lineage(rev, "reviews"), "film_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 14. Validation: PK uniqueness, orphan rate, date sanity
# MAGIC

# COMMAND ----------

# === 14. Validation checks ===
def one(q): return spark.sql(q).collect()[0]

for tbl, pk in [("movies","film_id"),("people","person_id"),("film_cast","credit_id"),("film_crew","credit_id"),("film_reviews","review_id")]:
    dups = one(f"SELECT COUNT(*) c FROM (SELECT {pk} FROM milkmoo.silver.{tbl} GROUP BY {pk} HAVING COUNT(*)>1)").c
    print(f"dup {tbl}.{pk}: {dups}")

for tbl in ["film_cast","film_crew"]:
    r = one(f"SELECT COUNT(*) total, SUM(CASE WHEN p.person_id IS NULL THEN 1 ELSE 0 END) orphans FROM milkmoo.silver.{tbl} c LEFT JOIN milkmoo.silver.people p ON c.person_id=p.person_id")
    pct = 100*r.orphans/r.total if r.total else 0
    print(f"{tbl}: {r.orphans:,}/{r.total:,} orphan person refs ({pct:.1f}%)")

r = one("SELECT MIN(release_date) lo, MAX(release_date) hi FROM milkmoo.silver.movies")
print("release_date range:", r.lo, "->", r.hi)