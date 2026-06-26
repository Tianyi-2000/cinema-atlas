# Databricks notebook source
# config
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TMDB_CATALOG = "milkmoo"     # TMDB lives in teammate's catalog
IMDB_CATALOG = "workspace"   # IMDb lives in our catalog

# write target
OUT_CATALOG  = "workspace"
OUT_SCHEMA   = "silver"

def out(name): return f"{OUT_CATALOG}.{OUT_SCHEMA}.{name}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUT_CATALOG}.{OUT_SCHEMA}")
print("Config ready.")

# COMMAND ----------

# dedupe tmdb to latest snapshot per film, 
tmdb_raw = spark.table(f"{TMDB_CATALOG}.bronze.tmdb_movies_raw")

w = Window.partitionBy("id").orderBy(F.col("load_ts").desc())
tmdb_latest = (
    tmdb_raw
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
    .filter(F.col("imdb_id").isNotNull() & (F.col("imdb_id") != ""))
)

print(f"TMDB raw rows:                 {tmdb_raw.count():,}")
print(f"TMDB distinct films w/ imdb_id: {tmdb_latest.count():,}")

# COMMAND ----------

# select relevant imdb columns
imdb_basics = (
    spark.table(f"{IMDB_CATALOG}.bronze.imdb_basics_validated")
    .select(
        F.col("tconst"),
        F.col("primaryTitle").alias("imdb_title"),
        F.col("startYear").cast("int").alias("imdb_year"),
        F.col("runtimeMinutes").cast("int").alias("imdb_runtime"),
        F.col("genres").alias("imdb_genres"),
    )
)
print(f"IMDb validated films: {imdb_basics.count():,}")


# COMMAND ----------

# inner join on tmdb.imdb_id == imdb.tconst
films = (
    tmdb_latest.alias("t")
    .join(imdb_basics.alias("i"), F.col("t.imdb_id") == F.col("i.tconst"), "inner")
    .select(
        F.col("t.id").alias("id"),                      # PK — TMDB native id
        F.col("t.id").alias("tmdb_id"),                 # explicit tmdb id
        F.col("t.imdb_id").alias("tconst"),             # IMDb id (== tconst)
        F.col("t.title").alias("title"),                # TMDB canonical title
        F.year(F.try_to_date("t.release_date")).alias("year"),
        F.col("t.original_title"),
        F.col("t.original_language"),
        F.col("t.overview"),
        F.col("t.runtime").alias("tmdb_runtime"),
        F.col("t.budget"),
        F.col("t.revenue"),
        F.col("t.popularity"),
        F.col("t.vote_average"),
        F.col("t.vote_count"),
        # IMDb-side, kept for lineage / fallback
        F.col("i.imdb_title"),
        F.col("i.imdb_year"),
        F.col("i.imdb_runtime"),
        F.col("i.imdb_genres"),
        F.current_timestamp().alias("merged_ts"),
    )
)

film_count = films.count()
print(f"Matched films (intersection): {film_count:,}")

# COMMAND ----------

# write silver.films
films.write.format("delta").mode("overwrite") \
     .option("overwriteSchema", "true") \
     .saveAsTable(out("films"))
print(f"Wrote {out('films')}: {spark.table(out('films')).count():,} rows")

# COMMAND ----------

# write matched tconsts
matched_tconsts = films.select("tconst", F.col("id").alias("film_id")).distinct()
matched_tconsts.write.format("delta").mode("overwrite") \
     .option("overwriteSchema", "true") \
     .saveAsTable(out("matched_tconsts"))
print(f"Wrote {out('matched_tconsts')}: {spark.table(out('matched_tconsts')).count():,} tconsts")

# COMMAND ----------

# sanity checks
films_t = spark.table(out("films"))

# 1. no null tconst (every film must have the IMDb key)
null_tconst = films_t.filter(F.col("tconst").isNull()).count()
print(f"Films with null tconst: {null_tconst}  (expect 0)")

# 2. tconst is unique (no film appears twice)
dup_tconst = films_t.groupBy("tconst").count().filter("count > 1").count()
print(f"Duplicate tconsts:      {dup_tconst}  (expect 0)")

# 3. id is unique
dup_id = films_t.groupBy("id").count().filter("count > 1").count()
print(f"Duplicate ids:          {dup_id}  (expect 0)")

# 4. spot check a known post-2000 film (Memento, tt0209144)
print("\nSpot check — Memento (tt0209144):")
films_t.filter(F.col("tconst") == "tt0209144") \
       .select("id", "tconst", "title", "year", "imdb_title", "imdb_year").show(truncate=False)