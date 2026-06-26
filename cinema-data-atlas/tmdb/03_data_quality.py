# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime

CATALOG = "milkmoo"
BRONZE  = "bronze"
today   = datetime.now().strftime("%Y-%m-%d")

ENDPOINTS = ["movies", "people", "credits", "releases", "reviews", "images"]

print("DQ config loaded —", today)

# COMMAND ----------

def latest_per_key(table, key="id"):
    w = Window.partitionBy(key).orderBy(F.col("load_ts").desc())
    return (spark.table(f"{CATALOG}.{BRONZE}.{table}")
            .withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1")
            .drop("_rn"))

# collects log lines so the final summary shows everything in one place
dq_log = []
def log(msg):
    dq_log.append(msg)
    print(msg)

# COMMAND ----------

# --- dedup to latest snapshot per film ---
movies = latest_per_key("tmdb_movies_raw", "id")
total = movies.count()
log(f"\n=== MOVIES === ({total:,} films after dedup)")

# helper: safe date parse — empty/garbage → null instead of error
def safe_date(c):
    return F.try_to_date(F.substring(c, 1, 10), F.lit("yyyy-MM-dd"))

# --- HARD checks → quarantine (unusable rows) ---
quarantine = movies.where(F.col("id").isNull()) \
                   .withColumn("fail_reason", F.lit("null_id")) \
                   .withColumn("dq_run_date", F.lit(today))

q_count = quarantine.count()
clean = movies.where(F.col("id").isNotNull())

# --- SOFT checks → keep but log ---
parsed = clean.withColumn("_rd", safe_date(F.col("release_date")))

bad_release = parsed.where(
    F.col("_rd").isNull() |
    (F.col("_rd") < "2000-01-01") |
    (F.col("_rd") > today)
).count()

bad_vote    = clean.where((F.col("vote_average") < 0) | (F.col("vote_average") > 10)).count()
bad_runtime = clean.where(F.col("runtime") < 0).count()
null_title  = clean.where(F.col("title").isNull()).count()

log(f"  quarantined (null id):       {q_count}")
log(f"  soft: bad/missing release:   {bad_release}")
log(f"  soft: vote_average out 0-10: {bad_vote}")
log(f"  soft: negative runtime:      {bad_runtime}")
log(f"  soft: null title:            {null_title}")
log(f"  clean rows → validated:      {clean.count():,}")

# --- write outputs ---
clean.write.format("delta").mode("overwrite") \
     .option("overwriteSchema","true") \
     .saveAsTable(f"{CATALOG}.{BRONZE}.tmdb_movies_validated")

if q_count > 0:
    quarantine.write.format("delta").mode("append") \
              .option("mergeSchema","true") \
              .saveAsTable(f"{CATALOG}.{BRONZE}.tmdb_movies_quarantine")

# COMMAND ----------

def simple_dq(table, endpoint, extra_checks=None):
    df = latest_per_key(table, "id")
    total = df.count()
    log(f"\n=== {endpoint.upper()} === ({total:,} rows after dedup)")

    quarantine = df.where(F.col("id").isNull()) \
                   .withColumn("fail_reason", F.lit("null_id")) \
                   .withColumn("dq_run_date", F.lit(today))
    q_count = quarantine.count()
    clean = df.where(F.col("id").isNotNull())

    log(f"  quarantined (null id):  {q_count}")
    if extra_checks:
        extra_checks(clean)
    log(f"  clean rows → validated: {clean.count():,}")

    clean.write.format("delta").mode("overwrite") \
         .option("overwriteSchema","true") \
         .saveAsTable(f"{CATALOG}.{BRONZE}.tmdb_{endpoint}_validated")
    if q_count > 0:
        quarantine.write.format("delta").mode("append") \
                  .option("mergeSchema","true") \
                  .saveAsTable(f"{CATALOG}.{BRONZE}.tmdb_{endpoint}_quarantine")

# people — also check null name (soft)
def people_extra(clean):
    log(f"  soft: null name: {clean.where(F.col('name').isNull()).count()}")

simple_dq("tmdb_people_raw",   "people", people_extra)
simple_dq("tmdb_credits_raw",  "credits")
simple_dq("tmdb_releases_raw", "releases")
simple_dq("tmdb_reviews_raw",  "reviews")
simple_dq("tmdb_images_raw",   "images")

# COMMAND ----------

log("\n" + "="*50)
log(f"  03_data_quality — run summary  ({today})")
log("="*50)
for ep in ENDPOINTS:
    raw = spark.table(f"{CATALOG}.{BRONZE}.tmdb_{ep}_raw").count()
    val = spark.table(f"{CATALOG}.{BRONZE}.tmdb_{ep}_validated").count()
    try:
        q = spark.table(f"{CATALOG}.{BRONZE}.tmdb_{ep}_quarantine").count()
    except:
        q = 0
    log(f"  {ep:10s}: raw {raw:>7,}  →  validated {val:>7,}  |  quarantined {q}")
log("="*50)