# Databricks notebook source
# config
%pip install rapidfuzz
dbutils.library.restartPython()

# COMMAND ----------

# continued config
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType
import unicodedata, re

TMDB_CATALOG = "milkmoo"
IMDB_CATALOG = "workspace"
OUT_CATALOG  = "workspace"
OUT_SCHEMA   = "silver"

FUZZY_THRESHOLD = 90   # rapidfuzz token_sort_ratio 0-100; >=90 is a confident name match

def out(name): return f"{OUT_CATALOG}.{OUT_SCHEMA}.{name}"

# name normalization: lowercase, strip accents, strip punctuation, collapse spaces
def _normalize(s):
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9 ]", " ", s).lower()
    return re.sub(r"\s+", " ", s).strip()

normalize_udf = F.udf(_normalize, StringType())
print("Config ready.")

# COMMAND ----------

# load matched-film scope
matched = spark.table(out("matched_tconsts"))   # tconst, film_id
print(f"Matched films: {matched.count():,}")

# COMMAND ----------

# pass 1: direct imdb_id → nconst join
tmdb_people = spark.table(f"{TMDB_CATALOG}.silver.people")

pass1 = (
    tmdb_people
    .filter(F.col("imdb_id").isNotNull() & (F.col("imdb_id") != ""))
    .select(
        F.col("person_id").alias("tmdb_person_id"),
        F.col("name").alias("tmdb_name"),
        F.col("imdb_id").alias("nconst"),
        F.lit("imdb_id_direct").alias("method"),
        F.lit(1.0).alias("confidence"),
    )
)
print(f"Pass 1 (imdb_id_direct): {pass1.count():,} people")

# COMMAND ----------

# Pass 2 setup: build film-anchored candidate pairs
# TMDB people still unresolved after pass 1
# TMDB people still unresolved after pass 1
resolved_ids = pass1.select("tmdb_person_id")
tmdb_unresolved = (
    tmdb_people
    .join(resolved_ids, tmdb_people.person_id == resolved_ids.tmdb_person_id, "left_anti")
    .select(F.col("person_id").alias("tmdb_person_id"), F.col("name").alias("tmdb_name"))
)

# TMDB crew -> film -> tconst (only matched films)
fc = spark.table(f"{TMDB_CATALOG}.silver.film_crew").select("person_id", "film_id").alias("fc")
m  = matched.alias("m")

tmdb_crew = (
    fc.join(m, F.col("fc.film_id") == F.col("m.film_id"), "inner")
    .select(F.col("fc.person_id").alias("tmdb_person_id"), F.col("m.tconst").alias("tconst"))
    .join(tmdb_unresolved, "tmdb_person_id", "inner")
    .select("tmdb_person_id", "tmdb_name", "tconst")
    .distinct()
)

# IMDb below-the-line crew on those same tconsts, with names
imdb_crew = (
    spark.table(f"{IMDB_CATALOG}.bronze.imdb_principals_validated")
    .join(matched, "tconst", "inner")
    .select("tconst", "nconst")
    .distinct()
    .join(
        spark.table(f"{IMDB_CATALOG}.bronze.imdb_names_validated")
            .select("nconst", F.col("primaryName").alias("imdb_name")),
        "nconst", "inner"
    )
)

# candidate pairs: same tconst
candidates = (
    tmdb_crew.join(imdb_crew, "tconst", "inner")
    .select("tmdb_person_id", "tmdb_name", "nconst", "imdb_name")
    .distinct()
)
print(f"Candidate pairs (same film): {candidates.count():,}")


# COMMAND ----------

# Pass 2 scoring: fuzzy name match on candidate pairs
from rapidfuzz import fuzz

def _score(a, b):
    if not a or not b:
        return 0.0
    return float(fuzz.token_sort_ratio(a, b))

score_udf = F.udf(_score, DoubleType())

scored = (
    candidates
    .withColumn("tmdb_norm", normalize_udf("tmdb_name"))
    .withColumn("imdb_norm", normalize_udf("imdb_name"))
    .withColumn("score", score_udf("tmdb_norm", "imdb_norm"))
    .filter(F.col("score") >= FUZZY_THRESHOLD)
)

# best IMDb match per TMDB person (highest score)
w = Window.partitionBy("tmdb_person_id").orderBy(F.col("score").desc())
pass2 = (
    scored
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .select(
        "tmdb_person_id",
        F.col("tmdb_name"),
        "nconst",
        F.lit("film_anchored").alias("method"),
        (F.col("score") / 100.0).alias("confidence"),
    )
)
print(f"Pass 2 (film_anchored): {pass2.count():,} people")

# COMMAND ----------

# combine resolved (Pass 1 + Pass 2)
resolved = pass1.unionByName(pass2)

# guard: a single nconst should map to one tmdb person. If pass2 produced a
# collision with pass1, pass1 (confidence 1.0) wins.
w2 = Window.partitionBy("nconst").orderBy(F.col("confidence").desc())
resolved = (
    resolved
    .withColumn("_rn", F.row_number().over(w2))
    .filter("_rn = 1").drop("_rn")
)
print(f"Total resolved (both passes): {resolved.count():,}")

# COMMAND ----------

# build people_resolved
# all TMDB people connected to matched films (cast + crew)
tmdb_in_scope = (
    spark.table(f"{TMDB_CATALOG}.silver.film_crew").select("person_id", "film_id")
    .union(spark.table(f"{TMDB_CATALOG}.silver.film_cast").select("person_id", "film_id"))
    .join(matched, "film_id", "inner")
    .select(F.col("person_id").alias("tmdb_person_id")).distinct()
)

# all IMDb crew on matched films
imdb_in_scope = (
    spark.table(f"{IMDB_CATALOG}.bronze.imdb_principals_validated")
    .join(matched, "tconst", "inner")
    .select("nconst").distinct()
)

# start from resolved, then left-join scope to find singles
res = resolved.select("tmdb_person_id", "nconst", "method", "confidence")

# tmdb-only: in tmdb scope, not in resolved
tmdb_only = (
    tmdb_in_scope.join(res.select("tmdb_person_id"), "tmdb_person_id", "left_anti")
    .select(
        "tmdb_person_id",
        F.lit(None).cast("string").alias("nconst"),
        F.lit("tmdb_only").alias("method"),
        F.lit(None).cast("double").alias("confidence"),
    )
)

# imdb-only: in imdb scope, not in resolved
imdb_only = (
    imdb_in_scope.join(res.select("nconst"), "nconst", "left_anti")
    .select(
        F.lit(None).cast("long").alias("tmdb_person_id"),
        "nconst",
        F.lit("imdb_only").alias("method"),
        F.lit(None).cast("double").alias("confidence"),
    )
)

people_resolved = res.unionByName(tmdb_only).unionByName(imdb_only)

# attach a source label + names/attributes from TMDB where available
people_resolved = people_resolved.withColumn(
    "source",
    F.when(F.col("method").isin("imdb_id_direct", "film_anchored"), F.lit("both"))
     .otherwise(F.col("method"))
)

people_resolved.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(out("people_resolved"))

print(f"Wrote {out('people_resolved')}: {spark.table(out('people_resolved')).count():,} rows")

# COMMAND ----------

# summary, sanity checks
pr = spark.table(out("people_resolved"))

print("=== People Resolution Summary ===")
pr.groupBy("method").count().orderBy(F.col("count").desc()).show()

print("Source breakdown:")
pr.groupBy("source").count().show()

# sanity: no nconst maps to two tmdb people
dup_nconst = pr.filter(F.col("nconst").isNotNull()) \
               .groupBy("nconst").count().filter("count > 1").count()
print(f"nconst mapped to >1 person: {dup_nconst}  (expect 0)")

# sanity: no tmdb person maps to two nconsts
dup_person = pr.filter(F.col("tmdb_person_id").isNotNull()) \
               .groupBy("tmdb_person_id").count().filter("count > 1").count()
print(f"tmdb_person mapped to >1 nconst: {dup_person}  (expect 0)")

# spot check a film-anchored match to eyeball quality
print("\nSample film_anchored matches (eyeball name quality):")
spark.table(out("people_resolved")) \
    .filter(F.col("method") == "film_anchored") \
    .show(10, truncate=False)

# COMMAND ----------

spark.table("workspace.silver.people_resolved") \
    .filter(F.col("method") == "film_anchored") \
    .groupBy("confidence").count().orderBy("confidence").show()

# COMMAND ----------

pr = spark.table("workspace.silver.people_resolved").filter(F.col("method") == "film_anchored")
tmdb_names = spark.table("milkmoo.silver.people").select(F.col("person_id").alias("tmdb_person_id"), F.col("name").alias("tmdb_name"))
imdb_names = spark.table("workspace.bronze.imdb_names_validated").select("nconst", F.col("primaryName").alias("imdb_name"))

pr.join(tmdb_names, "tmdb_person_id").join(imdb_names, "nconst") \
  .select("tmdb_name", "imdb_name", "confidence") \
  .orderBy("confidence").show(20, truncate=False)

# COMMAND ----------

spark.table("workspace.silver.films").printSchema()
spark.table("workspace.silver.people_resolved").printSchema()