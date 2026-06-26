# Databricks notebook source
# DBTITLE 1,Config
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

CATALOG = "workspace"
BRONZE  = "bronze"
SILVER  = "silver"
today   = datetime.now().strftime("%Y-%m-%d")

def src(name): return f"{CATALOG}.{BRONZE}.imdb_{name}_validated"
def out(name): return f"{CATALOG}.{SILVER}.{name}"

# matched_tconsts — bridge between tconst and film_id
matched = spark.table(f"{CATALOG}.{SILVER}.matched_tconsts")

print(f"Config ready — {today}")
print(f"  matched_tconsts: {matched.count():,} films")

# COMMAND ----------

# DBTITLE 1,imdb_film_ratings
# imdb_film_ratings
# Latest IMDb rating per film, joined to film_id via matched_tconsts
# ratings are append-only snapshots — dedupe to latest per tconst

ratings_raw = spark.table(src("ratings"))

# keep latest snapshot per tconst
w = Window.partitionBy("tconst").orderBy(F.col("snapshot_date").desc())
ratings_latest = (
    ratings_raw
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn", "source")
)

imdb_film_ratings = (
    ratings_latest
    .join(matched, "tconst", "inner")
    .select(
        F.col("film_id"),
        F.col("tconst"),
        F.col("averageRating").cast("double").alias("imdb_rating"),
        F.col("numVotes").cast("long").alias("imdb_votes"),
        F.col("snapshot_date").alias("rating_snapshot_date"),
        F.current_timestamp().alias("loaded_at")
    )
)

imdb_film_ratings.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("imdb_film_ratings"))
print(f"  imdb_film_ratings: {spark.table(out('imdb_film_ratings')).count():,} rows")

# COMMAND ----------

# DBTITLE 1,imdb_film_crew
# imdb_film_crew
# Below-the-line crew (cinematographer, editor, composer, etc.)
# joined to film_id via matched_tconsts

principals = spark.table(src("principals"))

CREW_CATEGORIES = {
    "cinematographer", "editor", "composer",
    "production_designer", "costume_designer"
}

imdb_film_crew = (
    principals
    .filter(F.col("category").isin(*CREW_CATEGORIES))
    .join(matched, principals.tconst == matched.tconst, "inner")
    .select(
        F.col("film_id"),
        matched.tconst,
        F.col("nconst"),
        F.col("category").alias("crew_role"),
        F.col("job"),
        F.col("ordering").cast("int"),
        F.current_timestamp().alias("loaded_at")
    )
    .distinct()
)

imdb_film_crew.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("imdb_film_crew"))
print(f"  imdb_film_crew: {spark.table(out('imdb_film_crew')).count():,} rows")

# COMMAND ----------

# DBTITLE 1,imdb_people
# imdb_people
# People from IMDb names table scoped to nconsts
# that appear in our matched film crew

names = spark.table(src("names"))
crew_nconsts = spark.table(out("imdb_film_crew")).select("nconst").distinct()

imdb_people = (
    names
    .join(crew_nconsts, "nconst", "inner")
    .select(
        F.col("nconst"),
        F.col("primaryName").alias("name"),
        F.col("birthYear").cast("int").alias("birth_year"),
        F.col("deathYear").cast("int").alias("death_year"),
        F.col("primaryProfession").alias("primary_profession"),
        F.current_timestamp().alias("loaded_at")
    )
    .distinct()
)

imdb_people.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("imdb_people"))
print(f"  imdb_people: {spark.table(out('imdb_people')).count():,} rows")

# COMMAND ----------

# DBTITLE 1,imdb_film_akas
# imdb_film_akas
# Alternate titles by region/language for matched films

akas = spark.table(src("akas"))

imdb_film_akas = (
    akas
    .join(matched, akas.titleId == matched.tconst, "inner")
    .select(
        F.col("film_id"),
        matched.tconst,
        F.col("title").alias("aka_title"),
        F.col("region"),
        F.col("language"),
        F.col("types").alias("title_type"),
        F.col("isOriginalTitle").cast("boolean").alias("is_original_title"),
        F.col("ordering").cast("int"),
        F.current_timestamp().alias("loaded_at")
    )
    .distinct()
)

imdb_film_akas.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("imdb_film_akas"))
print(f"  imdb_film_akas: {spark.table(out('imdb_film_akas')).count():,} rows")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 55)
print(f"  07_silver_imdb — run summary ({today})")
print("=" * 55)

tables = [
    "imdb_film_ratings",
    "imdb_film_crew",
    "imdb_people",
    "imdb_film_akas",
]

for t in tables:
    count = spark.table(out(t)).count()
    print(f"  {t:<30} {count:>8,} rows")

print("=" * 55)

# coverage check
total_matched = matched.count()
with_rating = spark.table(out("imdb_film_ratings")).select("film_id").distinct().count()
with_crew   = spark.table(out("imdb_film_crew")).select("film_id").distinct().count()
with_akas   = spark.table(out("imdb_film_akas")).select("film_id").distinct().count()

print(f"\n  Coverage vs {total_matched:,} matched films:")
print(f"  with IMDb rating : {with_rating:>6,} ({with_rating/total_matched*100:.1f}%)")
print(f"  with IMDb crew   : {with_crew:>6,} ({with_crew/total_matched*100:.1f}%)")
print(f"  with IMDb akas   : {with_akas:>6,} ({with_akas/total_matched*100:.1f}%)")