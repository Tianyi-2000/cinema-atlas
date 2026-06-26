# Databricks notebook source
# DBTITLE 1,Config
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

CATALOG = "workspace"
SCHEMA  = "bronze"
today   = datetime.now().strftime("%Y-%m-%d")

def src(name):        return f"{CATALOG}.{SCHEMA}.wikidata_{name}"
def validated(name):  return f"{CATALOG}.{SCHEMA}.wikidata_{name}_validated"
def quarantine(name): return f"{CATALOG}.{SCHEMA}.wikidata_{name}_quarantine"

def dedupe_latest(df, keys):
    """Keep only the latest row per key combination based on load_ts."""
    if "load_ts" in df.columns:
        w = Window.partitionBy(*keys).orderBy(F.col("load_ts").desc())
        return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    else:
        return df.dropDuplicates(keys)

def write_split(valid_df, invalid_df, name):
    """Overwrite validated table, append to quarantine."""
    v = valid_df.count()
    q = invalid_df.count()
    valid_df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(validated(name))
    if q > 0:
        invalid_df.write.format("delta").mode("append") \
            .option("mergeSchema", "true").saveAsTable(quarantine(name))
    print(f"  {name:<20} validated: {v:>8,}   quarantined: {q:>6,}")
    return v, q

dq_log = []
def log(msg):
    dq_log.append(msg)
    print(msg)

print("DQ config loaded —", today)

# COMMAND ----------

# DBTITLE 1,imdb_ids
df = spark.table(src("imdb_ids"))
df = dedupe_latest(df, ["wikidata_id", "imdb_id"])
total = df.count()
log(f"\n=== IMDB_IDS === ({total:,} rows after dedup)")

# HARD checks -> quarantine
invalid = df.filter(
    F.col("wikidata_id").isNull() |
    F.col("imdb_id").isNull()
).withColumn("fail_reason", F.lit("null_wikidata_id_or_imdb_id")) \
 .withColumn("dq_run_date", F.lit(today))

valid = df.filter(
    F.col("wikidata_id").isNotNull() &
    F.col("imdb_id").isNotNull()
)

# SOFT checks -> log only
bad_imdb_fmt = valid.filter(~F.col("imdb_id").rlike("^tt[0-9]+$")).count()
bad_year     = valid.filter(
    F.col("year").isNull() |
    (F.col("year").cast("int") < 2000) |
    (F.col("year").cast("int") > 2030)
).count()

log(f"  quarantined (null wikidata_id or imdb_id): {invalid.count()}")
log(f"  soft: imdb_id format mismatch (not tt[0-9]+): {bad_imdb_fmt}")
log(f"  soft: year out of range (2000-2030):           {bad_year}")

write_split(valid, invalid, "imdb_ids")

# COMMAND ----------

# DBTITLE 1,movements
df = spark.table(src("movements"))
df = dedupe_latest(df, ["wikidata_id", "movement_wikidata_id"])
total = df.count()
log(f"\n=== MOVEMENTS === ({total:,} rows after dedup)")

# HARD checks -> quarantine
invalid = df.filter(
    F.col("wikidata_id").isNull() |
    F.col("movement_wikidata_id").isNull()
).withColumn("fail_reason", F.lit("null_wikidata_id_or_movement_id")) \
 .withColumn("dq_run_date", F.lit(today))

valid = df.filter(
    F.col("wikidata_id").isNotNull() &
    F.col("movement_wikidata_id").isNotNull()
)

# SOFT checks -> log only
null_label = valid.filter(F.col("movement_label").isNull()).count()
log(f"  quarantined (null wikidata_id or movement_id): {invalid.count()}")
log(f"  soft: null movement_label: {null_label}")

write_split(valid, invalid, "movements")

# COMMAND ----------

# DBTITLE 1,festivals
df = spark.table(src("festivals"))
df = dedupe_latest(df, ["wikidata_id", "festival_wikidata_id"])
total = df.count()
log(f"\n=== FESTIVALS === ({total:,} rows after dedup)")

# HARD checks -> quarantine
invalid = df.filter(
    F.col("wikidata_id").isNull() |
    F.col("festival_wikidata_id").isNull()
).withColumn("fail_reason", F.lit("null_wikidata_id_or_festival_id")) \
 .withColumn("dq_run_date", F.lit(today))

valid = df.filter(
    F.col("wikidata_id").isNotNull() &
    F.col("festival_wikidata_id").isNotNull()
)

# SOFT checks -> log only
null_label = valid.filter(F.col("festival_label").isNull()).count()
log(f"  quarantined (null wikidata_id or festival_id): {invalid.count()}")
log(f"  soft: null festival_label: {null_label}")

write_split(valid, invalid, "festivals")

# COMMAND ----------

# DBTITLE 1,based_on
df = spark.table(src("based_on"))
df = dedupe_latest(df, ["wikidata_id", "source_wikidata_id"])
total = df.count()
log(f"\n=== BASED_ON === ({total:,} rows after dedup)")

# HARD checks -> quarantine
invalid = df.filter(
    F.col("wikidata_id").isNull() |
    F.col("source_wikidata_id").isNull()
).withColumn("fail_reason", F.lit("null_wikidata_id_or_source_id")) \
 .withColumn("dq_run_date", F.lit(today))

valid = df.filter(
    F.col("wikidata_id").isNotNull() &
    F.col("source_wikidata_id").isNotNull()
)

# SOFT checks -> log only
null_label = valid.filter(F.col("source_label").isNull()).count()
log(f"  quarantined (null wikidata_id or source_id): {invalid.count()}")
log(f"  soft: null source_label: {null_label}")

write_split(valid, invalid, "based_on")

# COMMAND ----------

# DBTITLE 1,influenced_by
df = spark.table(src("influenced_by"))
df = dedupe_latest(df, ["wikidata_id", "influence_wikidata_id"])
total = df.count()
log(f"\n=== INFLUENCED_BY === ({total:,} rows after dedup)")

# HARD checks -> quarantine
invalid = df.filter(
    F.col("wikidata_id").isNull() |
    F.col("influence_wikidata_id").isNull()
).withColumn("fail_reason", F.lit("null_wikidata_id_or_influence_id")) \
 .withColumn("dq_run_date", F.lit(today))

valid = df.filter(
    F.col("wikidata_id").isNotNull() &
    F.col("influence_wikidata_id").isNotNull()
)

# SOFT checks -> log only
null_label = valid.filter(F.col("influence_label").isNull()).count()
log(f"  quarantined (null wikidata_id or influence_id): {invalid.count()}")
log(f"  soft: null influence_label: {null_label}")

write_split(valid, invalid, "influenced_by")

# COMMAND ----------

# DBTITLE 1,Summary
log("\n" + "="*55)
log(f"  03_data_quality_wikidata — run summary ({today})")
log("="*55)
for name in ["imdb_ids", "movements", "festivals", "based_on", "influenced_by"]:
    raw = spark.table(src(name)).count()
    val = spark.table(validated(name)).count()
    try:
        q = spark.table(quarantine(name)).count()
    except Exception:
        q = 0
    log(f"  {name:<20} raw: {raw:>8,}  validated: {val:>8,}  quarantined: {q:>6,}")
log("="*55)