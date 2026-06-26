# Databricks notebook source
# DBTITLE 1,Config
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

CATALOG = "workspace"
BRONZE  = "bronze"
SILVER  = "silver"
today   = datetime.now().strftime("%Y-%m-%d")

def src(name): return f"{CATALOG}.{BRONZE}.wikidata_{name}_validated"
def out(name): return f"{CATALOG}.{SILVER}.{name}"

# load matched_tconsts — the bridge between imdb_id and film_id
matched = spark.table(f"{CATALOG}.{SILVER}.matched_tconsts")

print(f"Config ready — {today}")
print(f"  matched_tconsts: {matched.count():,} films")

# COMMAND ----------

# DBTITLE 1,wikidata_film_movements
# wikidata_film_movements
# dimension: unique movements
# bridge: film_id <-> movement

movements = spark.table(src("movements"))

# dimension table — unique movements
movements_dim = (
    movements
    .select(
        F.col("movement_wikidata_id"),
        F.col("movement_label")
    )
    .distinct()
    .withColumn("loaded_at", F.current_timestamp())
)

movements_dim.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("wikidata_movements"))
print(f"  wikidata_movements (dim): {spark.table(out('wikidata_movements')).count()} rows")

# bridge table — film_id <-> movement
film_movements = (
    movements
    .join(matched, movements.imdb_id == matched.tconst, "inner")
    .select(
        F.col("film_id"),
        F.col("movement_wikidata_id"),
        F.col("movement_label"),
        F.col("method"),
        F.col("confidence"),
        F.current_timestamp().alias("loaded_at")
    )
    .distinct()
)

film_movements.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("wikidata_film_movements"))
print(f"  wikidata_film_movements (bridge): {spark.table(out('wikidata_film_movements')).count()} rows")

# COMMAND ----------

# DBTITLE 1,wikidata_film_festivals
# wikidata_film_festivals
# dimension: unique festivals
# bridge: film_id <-> festival

festivals = spark.table(src("festivals"))

# dimension table — unique festivals
festivals_dim = (
    festivals
    .select(
        F.col("festival_wikidata_id"),
        F.col("festival_label")
    )
    .distinct()
    .withColumn("loaded_at", F.current_timestamp())
)

festivals_dim.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("wikidata_festivals"))
print(f"  wikidata_festivals (dim): {spark.table(out('wikidata_festivals')).count()} rows")

# bridge table — film_id <-> festival
film_festivals = (
    festivals
    .join(matched, festivals.imdb_id == matched.tconst, "inner")
    .select(
        F.col("film_id"),
        F.col("festival_wikidata_id"),
        F.col("festival_label"),
        F.col("method"),
        F.current_timestamp().alias("loaded_at")
    )
    .distinct()
)

film_festivals.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("wikidata_film_festivals"))
print(f"  wikidata_film_festivals (bridge): {spark.table(out('wikidata_film_festivals')).count()} rows")

# COMMAND ----------

# DBTITLE 1,wikidata_film_based_on
# wikidata_film_based_on
# maps film_id to its source material (book, story, etc.)

based_on = spark.table(src("based_on"))

film_based_on = (
    based_on
    .join(matched, based_on.imdb_id == matched.tconst, "inner")
    .select(
        F.col("film_id"),
        F.col("source_wikidata_id"),
        F.col("source_label"),
        F.col("method"),
        F.current_timestamp().alias("loaded_at")
    )
    .distinct()
)

film_based_on.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("wikidata_film_based_on"))
print(f"  wikidata_film_based_on: {spark.table(out('wikidata_film_based_on')).count():,} rows")

# COMMAND ----------

# DBTITLE 1,wikidata_film_influences
# wikidata_film_influences
# maps film_id to works/people that influenced it

influenced_by = spark.table(src("influenced_by"))

film_influences = (
    influenced_by
    .join(matched, influenced_by.imdb_id == matched.tconst, "inner")
    .select(
        F.col("film_id"),
        F.col("influence_wikidata_id"),
        F.col("influence_label"),
        F.col("method"),
        F.col("confidence"),
        F.current_timestamp().alias("loaded_at")
    )
    .distinct()
)

film_influences.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(out("wikidata_film_influences"))
print(f"  wikidata_film_influences: {spark.table(out('wikidata_film_influences')).count():,} rows")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 55)
print(f"  06_silver_wikidata — run summary ({today})")
print("=" * 55)

tables = [
    "wikidata_movements",
    "wikidata_film_movements",
    "wikidata_festivals",
    "wikidata_film_festivals",
    "wikidata_film_based_on",
    "wikidata_film_influences",
]

for t in tables:
    count = spark.table(out(t)).count()
    print(f"  {t:<35} {count:>8,} rows")

print("=" * 55)

# coverage check — how many matched films have wikidata enrichment
total_matched = matched.count()
with_festival  = spark.table(out("wikidata_film_festivals")).select("film_id").distinct().count()
with_based_on  = spark.table(out("wikidata_film_based_on")).select("film_id").distinct().count()
with_movement  = spark.table(out("wikidata_film_movements")).select("film_id").distinct().count()
with_influence = spark.table(out("wikidata_film_influences")).select("film_id").distinct().count()

print(f"\n  Coverage vs {total_matched:,} matched films:")
print(f"  with festival data  : {with_festival:>6,} ({with_festival/total_matched*100:.1f}%)")
print(f"  with based_on data  : {with_based_on:>6,} ({with_based_on/total_matched*100:.1f}%)")
print(f"  with movement data  : {with_movement:>6,} ({with_movement/total_matched*100:.1f}%)")
print(f"  with influence data : {with_influence:>6,} ({with_influence/total_matched*100:.1f}%)")