# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

CATALOG = "milkmoo"
BRONZE  = "bronze"
SILVER  = "silver"
today   = datetime.now().strftime("%Y-%m-%d")

def to_date10(c):     return F.try_to_date(F.substring(c, 1, 10), F.lit("yyyy-MM-dd"))
def empty_to_null(c): return F.when(F.trim(c) == "", None).otherwise(c)

def add_lineage(df, endpoint):
    return (df.withColumn("source_system",   F.lit("tmdb"))
              .withColumn("source_endpoint", F.lit(endpoint))
              .withColumn("source_file",     F.col("source_file") if "source_file" in df.columns else F.lit(None).cast("string"))
              .withColumn("loaded_at",       F.current_timestamp()))

def merge_scd1(df, table, keys, null_safe_keys=None):
    """SCD Type 1. null_safe_keys use <=> so null=null matches."""
    null_safe_keys = null_safe_keys or []
    full = f"{CATALOG}.{SILVER}.{table}"
    before = spark.table(full).count()
    tgt = DeltaTable.forName(spark, full)
    parts = []
    for k in keys:
        op = "<=>" if k in null_safe_keys else "="
        parts.append(f"t.{k} {op} s.{k}")
    cond = " AND ".join(parts)
    (tgt.alias("t").merge(df.alias("s"), cond)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    after = spark.table(full).count()
    print(f"  {table:30s}: {before:,} → {after:,} (+{after-before:,})")

print("Silver incremental config loaded —", today)

# COMMAND ----------

mv  = spark.read.table(f"{CATALOG}.{BRONZE}.tmdb_movies_validated")
ppl = spark.read.table(f"{CATALOG}.{BRONZE}.tmdb_people_validated")
cr  = spark.read.table(f"{CATALOG}.{BRONZE}.tmdb_credits_validated")
rel = spark.read.table(f"{CATALOG}.{BRONZE}.tmdb_releases_validated")
rev = spark.read.table(f"{CATALOG}.{BRONZE}.tmdb_reviews_validated")

print("validated bronze loaded")
print(f"  movies:   {mv.count():,}")
print(f"  people:   {ppl.count():,}")
print(f"  credits:  {cr.count():,}")
print(f"  releases: {rel.count():,}")
print(f"  reviews:  {rev.count():,}")

# COMMAND ----------

movies = (mv.select(
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
    F.get_json_object(F.col("belongs_to_collection"), "$.id").cast("bigint").alias("collection_id"),
    F.get_json_object(F.col("belongs_to_collection"), "$.name").alias("collection_name"),
    F.col("poster_path"), F.col("backdrop_path"),
    F.col("source_file"),
).dropDuplicates(["film_id"]))

movies = add_lineage(movies, "movies")
merge_scd1(movies, "movies", ["film_id"])

# COMMAND ----------

people = (ppl.select(
    F.col("id").cast("bigint").alias("person_id"),
    empty_to_null(F.col("imdb_id")).alias("imdb_id"),
    F.col("name"), F.col("gender").cast("int").alias("gender"),
    to_date10(F.col("birthday")).alias("birthday"),
    to_date10(F.col("deathday")).alias("deathday"),
    F.col("known_for_department"),
    empty_to_null(F.col("place_of_birth")).alias("place_of_birth"),
    F.col("popularity").cast("double").alias("popularity"),
    F.col("biography"), F.col("profile_path"),
    F.col("source_file"),
).dropDuplicates(["person_id"]))
merge_scd1(add_lineage(people, "people"), "people", ["person_id"])

aliases = (ppl.select(
    F.col("id").cast("bigint").alias("person_id"),
    F.explode_outer("also_known_as").alias("alias"),
    F.col("source_file"))
    .where(F.col("alias").isNotNull() & (F.trim(F.col("alias")) != ""))
    .dropDuplicates(["person_id", "alias"]))
merge_scd1(add_lineage(aliases, "people"), "person_aliases", ["person_id", "alias"])

# COMMAND ----------

cast = (cr.select(
        F.col("id").cast("bigint").alias("film_id"),
        F.explode("cast").alias("c"),
        F.col("source_file"))
    .select(
        F.col("c.credit_id").alias("credit_id"), "film_id",
        F.col("c.id").cast("bigint").alias("person_id"),
        F.col("c.character").alias("character"),
        F.col("c.order").cast("int").alias("cast_order"),
        F.col("c.cast_id").cast("int").alias("cast_id"),
        F.col("source_file"))
    .dropDuplicates(["credit_id"]))
merge_scd1(add_lineage(cast, "credits"), "film_cast", ["credit_id"])

crew = (cr.select(
        F.col("id").cast("bigint").alias("film_id"),
        F.explode("crew").alias("c"),
        F.col("source_file"))
    .select(
        F.col("c.credit_id").alias("credit_id"), "film_id",
        F.col("c.id").cast("bigint").alias("person_id"),
        F.col("c.department").alias("department"),
        F.col("c.job").alias("job"),
        F.col("source_file"))
    .dropDuplicates(["credit_id"]))
merge_scd1(add_lineage(crew, "credits"), "film_crew", ["credit_id"])

# COMMAND ----------

# genres
gx = mv.select(F.col("id").cast("bigint").alias("film_id"), F.explode("genres").alias("g"))
genres = gx.select(F.col("g.id").cast("int").alias("genre_id"), F.col("g.name").alias("genre_name")).dropDuplicates(["genre_id"])
film_genres = gx.select("film_id", F.col("g.id").cast("int").alias("genre_id")).dropDuplicates(["film_id","genre_id"])
merge_scd1(add_lineage(genres, "movies"), "genres", ["genre_id"])
merge_scd1(add_lineage(film_genres, "movies"), "film_genres", ["film_id","genre_id"])

# production companies
pcx = mv.select(F.col("id").cast("bigint").alias("film_id"), F.explode("production_companies").alias("c"))
companies = pcx.select(F.col("c.id").cast("bigint").alias("company_id"), F.col("c.name").alias("company_name"),
                       empty_to_null(F.col("c.origin_country")).alias("origin_country"), F.col("c.logo_path").alias("logo_path")).dropDuplicates(["company_id"])
film_companies = pcx.select("film_id", F.col("c.id").cast("bigint").alias("company_id")).dropDuplicates(["film_id","company_id"])
merge_scd1(add_lineage(companies, "movies"), "production_companies", ["company_id"])
merge_scd1(add_lineage(film_companies, "movies"), "film_production_companies", ["film_id","company_id"])

# production countries
pcox = mv.select(F.col("id").cast("bigint").alias("film_id"), F.explode("production_countries").alias("c"))
film_countries = pcox.select("film_id", F.col("c.iso_3166_1").alias("country_iso")).dropDuplicates(["film_id","country_iso"])
merge_scd1(add_lineage(film_countries, "movies"), "film_production_countries", ["film_id","country_iso"])

# languages
lx = mv.select(F.col("id").cast("bigint").alias("film_id"), F.explode("spoken_languages").alias("l"))
languages = lx.select(F.col("l.iso_639_1").alias("language_iso"), F.col("l.name").alias("language_name"), F.col("l.english_name").alias("english_name")).dropDuplicates(["language_iso"])
film_languages = lx.select("film_id", F.col("l.iso_639_1").alias("language_iso")).dropDuplicates(["film_id","language_iso"])
merge_scd1(add_lineage(languages, "movies"), "languages", ["language_iso"])
merge_scd1(add_lineage(film_languages, "movies"), "film_spoken_languages", ["film_id","language_iso"])

# countries
prod_c = mv.select(F.explode("production_countries").alias("c")).select(F.col("c.iso_3166_1").alias("country_iso"), F.col("c.name").alias("country_name"))
rel_c  = rel.select(F.explode("countries").alias("c")).select(F.col("c.iso_3166_1").alias("country_iso"), F.lit(None).cast("string").alias("country_name"))
countries = (prod_c.unionByName(rel_c).groupBy("country_iso").agg(F.max("country_name").alias("country_name")).where(F.col("country_iso").isNotNull()))
merge_scd1(add_lineage(countries, "movies"), "countries", ["country_iso"])

# COMMAND ----------

# film_releases
rel_df = (rel.select(
        F.col("id").cast("bigint").alias("film_id"),
        F.explode("countries").alias("c"),
        F.col("source_file"))
    .select("film_id",
        F.col("c.iso_3166_1").alias("country_iso"),
        empty_to_null(F.col("c.certification")).alias("certification"),
        to_date10(F.col("c.release_date")).alias("release_date"),
        F.col("c.primary").cast("boolean").alias("is_primary"),
        F.lit(None).cast("int").alias("release_type"),
        F.col("c.descriptors").alias("descriptors"),
        F.col("source_file"))
    .dropDuplicates(["film_id","country_iso","release_date","certification"]))

merge_scd1(add_lineage(rel_df, "releases"), "film_releases",
           ["film_id","country_iso","release_date","certification"],
           null_safe_keys=["release_date","certification"])

# film_reviews
rev_df = (rev.select(
        F.col("id").cast("bigint").alias("film_id"),
        F.explode("results").alias("r"),
        F.col("source_file"))
    .select(
        F.col("r.id").alias("review_id"), "film_id",
        F.col("r.author").alias("author"),
        F.col("r.author_details.username").alias("author_username"),
        F.col("r.author_details.rating").cast("double").alias("author_rating"),
        F.col("r.content").alias("content"),
        F.to_timestamp(F.col("r.created_at")).alias("created_at"),
        F.to_timestamp(F.col("r.updated_at")).alias("updated_at"),
        F.col("r.url").alias("url"),
        F.col("source_file"))
    .dropDuplicates(["review_id"]))

merge_scd1(add_lineage(rev_df, "reviews"), "film_reviews", ["review_id"])

# COMMAND ----------

from pyspark.sql import Window

# pull every snapshot from bronze raw (full history, not deduped)
snapshots = (spark.table("milkmoo.bronze.tmdb_movies_raw")
    .select(
        F.col("id").cast("bigint").alias("film_id"),
        F.col("popularity").cast("double").alias("popularity"),
        F.col("vote_average").cast("double").alias("vote_average"),
        F.col("vote_count").cast("int").alias("vote_count"),
        F.col("revenue").cast("bigint").alias("revenue"),
        F.col("budget").cast("bigint").alias("budget"),
        F.col("load_ts").alias("snapshot_ts"),
    )
    # one row per film per load_ts (dedup exact-duplicate snapshots)
    .dropDuplicates(["film_id", "snapshot_ts"]))

# mark the latest snapshot per film as current
w = Window.partitionBy("film_id").orderBy(F.col("snapshot_ts").desc())
snapshots = (snapshots
    .withColumn("_rn", F.row_number().over(w))
    .withColumn("is_current", F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("loaded_at", F.current_timestamp()))

# full rebuild of the SCD2 table (idempotent — safe to re-run)
snapshots.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("milkmoo.silver.audience_trends")

total = spark.table("milkmoo.silver.audience_trends").count()
current = spark.table("milkmoo.silver.audience_trends").where("is_current = true").count()
print(f"audience_trends: {total:,} total snapshots | {current:,} current rows")

# COMMAND ----------

from datetime import datetime
today = datetime.now().strftime("%Y-%m-%d")

silver_tables = [
    "movies", "people", "person_aliases",
    "film_cast", "film_crew",
    "genres", "film_genres",
    "production_companies", "film_production_companies",
    "film_production_countries",
    "languages", "film_spoken_languages", "countries",
    "film_releases", "film_reviews",
    "audience_trends",
]

print("="*55)
print(f"  04_silver_incremental — run summary  ({today})")
print("="*55)
for t in silver_tables:
    c = spark.table(f"milkmoo.silver.{t}").count()
    print(f"  silver.{t:30s}: {c:>9,} rows")

# SCD2 health check
cur = spark.table("milkmoo.silver.audience_trends").where("is_current = true").count()
tot = spark.table("milkmoo.silver.audience_trends").count()
print("-"*55)
print(f"  audience_trends: {tot:,} snapshots | {cur:,} current | {tot-cur:,} historical")
print("="*55)