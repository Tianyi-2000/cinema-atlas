# Databricks notebook source
# DBTITLE 1,Config
from pyspark.sql import functions as F
from datetime import datetime

CATALOG = "workspace"
SILVER  = "silver"
today   = datetime.now().strftime("%Y-%m-%d")

def tbl(name): return f"{CATALOG}.{SILVER}.{name}"

print(f"Config ready — {today}")

# COMMAND ----------

# DBTITLE 1,Load base tables
# base — already has TMDB + IMDb basics merged
films = spark.table(tbl("films"))

# IMDb enrichment
ratings = spark.table(tbl("imdb_film_ratings")) \
    .select("film_id", "imdb_rating", "imdb_votes", "rating_snapshot_date")

# Wikidata ID mapping (wikidata_id via imdb_id = tconst)
wikidata_ids = spark.table(f"{CATALOG}.bronze.wikidata_imdb_ids_validated") \
    .select(
        F.col("imdb_id").alias("tconst"),
        F.col("wikidata_id")
    ) \
    .dropDuplicates(["tconst"])  # keep one wikidata_id per tconst

# Wikidata relationship arrays — aggregate to one row per film
festivals = spark.table(tbl("wikidata_film_festivals")) \
    .groupBy("film_id") \
    .agg(F.collect_set("festival_label").alias("festivals"))

movements = spark.table(tbl("wikidata_film_movements")) \
    .groupBy("film_id") \
    .agg(F.collect_set("movement_label").alias("movements"))

based_on = spark.table(tbl("wikidata_film_based_on")) \
    .groupBy("film_id") \
    .agg(F.collect_set("source_label").alias("based_on"))

influences = spark.table(tbl("wikidata_film_influences")) \
    .groupBy("film_id") \
    .agg(F.collect_set("influence_label").alias("influences"))

print("Base tables loaded.")
print(f"  films          : {films.count():,}")
print(f"  ratings        : {ratings.count():,}")
print(f"  wikidata_ids   : {wikidata_ids.count():,}")
print(f"  festivals      : {festivals.count():,}")
print(f"  movements      : {movements.count():,}")
print(f"  based_on       : {based_on.count():,}")
print(f"  influences     : {influences.count():,}")

# COMMAND ----------

# DBTITLE 1,Build unified Silver
# join everything to films — all left joins so we never lose a film
unified = (
    films
    # IMDb ratings
    .join(ratings, films.id == ratings.film_id, "left")
    # Wikidata ID
    .join(wikidata_ids, "tconst", "left")
    # Wikidata arrays
    .join(festivals,  films.id == festivals.film_id,  "left")
    .join(movements,  films.id == movements.film_id,  "left")
    .join(based_on,   films.id == based_on.film_id,   "left")
    .join(influences, films.id == influences.film_id, "left")
    .select(
        # identity
        F.col("id").alias("film_id"),
        F.col("tconst"),
        F.col("wikidata_id"),
        # TMDB fields
        F.col("title"),
        F.col("original_title"),
        F.col("original_language"),
        F.col("year"),
        F.col("overview"),
        F.col("tmdb_runtime"),
        F.col("budget"),
        F.col("revenue"),
        F.col("popularity"),
        F.col("vote_average").alias("tmdb_rating"),
        F.col("vote_count").alias("tmdb_votes"),
        # IMDb fields
        F.col("imdb_title"),
        F.col("imdb_year"),
        F.col("imdb_runtime"),
        F.col("imdb_genres"),
        F.col("imdb_rating"),
        F.col("imdb_votes"),
        F.col("rating_snapshot_date"),
        # Wikidata arrays
        F.col("festivals"),
        F.col("movements"),
        F.col("based_on"),
        F.col("influences"),
        # lineage
        F.col("merged_ts"),
        F.current_timestamp().alias("unified_at")
    )
)

print(f"Unified Silver: {unified.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Write unified_silver
unified.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tbl("unified_silver"))

print(f"Written to {tbl('unified_silver')}: {spark.table(tbl('unified_silver')).count():,} rows")

# COMMAND ----------

# DBTITLE 1,Sanity checks
u = spark.table(tbl("unified_silver"))
total = u.count()

# coverage across sources
with_wikidata  = u.filter(F.col("wikidata_id").isNotNull()).count()
with_imdb_rtg  = u.filter(F.col("imdb_rating").isNotNull()).count()
with_festivals = u.filter(F.size(F.col("festivals")) > 0).count()
with_movements = u.filter(F.size(F.col("movements")) > 0).count()
with_based_on  = u.filter(F.size(F.col("based_on")) > 0).count()
with_influences= u.filter(F.size(F.col("influences")) > 0).count()

# no duplicate film_ids
dups = u.groupBy("film_id").count().filter("count > 1").count()

print(f"\n=== unified_silver sanity checks ===")
print(f"  Total films          : {total:,}")
print(f"  Duplicate film_ids   : {dups} (expect 0)")
print(f"\n  Source coverage:")
print(f"  with wikidata_id     : {with_wikidata:>6,} ({with_wikidata/total*100:.1f}%)")
print(f"  with imdb_rating     : {with_imdb_rtg:>6,} ({with_imdb_rtg/total*100:.1f}%)")
print(f"  with festivals       : {with_festivals:>6,} ({with_festivals/total*100:.1f}%)")
print(f"  with movements       : {with_movements:>6,} ({with_movements/total*100:.1f}%)")
print(f"  with based_on        : {with_based_on:>6,} ({with_based_on/total*100:.1f}%)")
print(f"  with influences      : {with_influences:>6,} ({with_influences/total*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Spot check
# spot check — The Prestige (tt0482571)
print("Spot check — The Prestige (tt0482571):")
(
    spark.table(tbl("unified_silver"))
    .filter(F.col("tconst") == "tt0482571")
    .select(
        "film_id", "tconst", "wikidata_id", "title", "year",
        "tmdb_rating", "tmdb_votes",
        "imdb_rating", "imdb_votes",
        "festivals", "based_on", "movements", "influences"
    )
    .show(1, truncate=False)
)