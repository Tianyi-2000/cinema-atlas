# Databricks notebook source
# Configuration

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "milkmoo"
SCHEMA  = "bronze"

def src(name):       return f"{CATALOG}.{SCHEMA}.imdb_{name}"
def validated(name): return f"{CATALOG}.{SCHEMA}.imdb_{name}_validated"
def quarantine(name):return f"{CATALOG}.{SCHEMA}.imdb_{name}_quarantine"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

def write_split(valid_df, invalid_df, name):
    """Overwrite the validated/quarantine tables for a source."""
    v, q = valid_df.count(), invalid_df.count()
    valid_df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(validated(name))
    invalid_df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(quarantine(name))
    print(f"  {name:<12} validated: {v:>8,}   quarantined: {q:>6,}")
    return v, q

def dedupe_latest(df, keys):
    if "load_ts" in df.columns:
        w = Window.partitionBy(*keys).orderBy(F.col("load_ts").desc())
        return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    else:
        return df.dropDuplicates(keys)

print("Config ready.")

# COMMAND ----------

# basics
df = spark.table(src("basics"))

df = dedupe_latest(df, ["tconst"])

valid = df.filter(
    F.col("tconst").isNotNull() &
    F.col("tconst").rlike("^tt[0-9]+$") &
    F.col("primaryTitle").isNotNull() &
    F.col("startYear").isNotNull() &
    (F.col("startYear").cast("int") >= 1880) &
    (F.col("startYear").cast("int") <= 2030)
)
invalid = df.subtract(valid)
write_split(valid, invalid, "basics")

# COMMAND ----------

print(spark.table("workspace.bronze.imdb_basics").columns)

# COMMAND ----------

# ratings
df = spark.table(src("ratings"))

# dedupe within (tconst, snapshot_date) — keep latest load_ts if a snapshot was ingested twice
df = dedupe_latest(df, ["tconst", "snapshot_date"])

valid = df.filter(
    F.col("tconst").isNotNull() &
    F.col("tconst").rlike("^tt[0-9]+$") &
    F.col("averageRating").isNotNull() &
    (F.col("averageRating").cast("double") >= 1.0) &
    (F.col("averageRating").cast("double") <= 10.0) &
    (F.col("numVotes").cast("long") >= 0)
)
invalid = df.subtract(valid)
write_split(valid, invalid, "ratings")

# COMMAND ----------

# akas
df = spark.table(src("akas"))

df = dedupe_latest(df, ["titleId", "ordering"])

valid = df.filter(
    F.col("titleId").isNotNull() &
    F.col("titleId").rlike("^tt[0-9]+$") &
    F.col("title").isNotNull()
)
invalid = df.subtract(valid)
write_split(valid, invalid, "akas")

# COMMAND ----------

# principals
df = spark.table(src("principals"))

CREW_CATEGORIES = ["cinematographer", "editor", "composer",
                   "production_designer", "costume_designer"]

df = dedupe_latest(df, ["tconst", "nconst", "ordering"])

valid = df.filter(
    F.col("tconst").isNotNull() & F.col("tconst").rlike("^tt[0-9]+$") &
    F.col("nconst").isNotNull() & F.col("nconst").rlike("^nm[0-9]+$") &
    F.col("category").isin(CREW_CATEGORIES)
)
invalid = df.subtract(valid)
write_split(valid, invalid, "principals")

# COMMAND ----------

# names
df = spark.table(src("names"))

df = dedupe_latest(df, ["nconst"])

valid = df.filter(
    F.col("nconst").isNotNull() &
    F.col("nconst").rlike("^nm[0-9]+$") &
    F.col("primaryName").isNotNull()
)
invalid = df.subtract(valid)
write_split(valid, invalid, "names")

# COMMAND ----------

print("=== IMDb Data Quality Summary ===\n")
for name in ["basics", "ratings", "akas", "principals", "names"]:
    v = spark.table(validated(name)).count()
    q = spark.table(quarantine(name)).count()
    total = v + q
    pct = (q / total * 100) if total else 0
    print(f"  {name:<12} validated {v:>8,}   quarantined {q:>6,}  ({pct:.2f}% bad)")

# COMMAND ----------

display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

spark.sql("USE CATALOG milkmoo")
display(spark.sql("SHOW SCHEMAS IN milkmoo"))
display(spark.sql("SHOW TABLES IN milkmoo.bronze"))