# Databricks notebook source
# DBTITLE 1,Config
import requests, json, time, os
from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import functions as F

CATALOG     = "workspace"
SCHEMA      = "bronze"
VOLUME      = f"/Volumes/{CATALOG}/{SCHEMA}/wikidata_raw"

SPARQL_URL  = "https://query.wikidata.org/sparql"
HEADERS     = {
    "User-Agent": "TruffleBlanche/1.0 (USF MSDS683 cinema KG project; contact: aatish.lobo@gmail.com)",
    "Accept": "application/json"
}
START_YEAR  = 2000
BATCH_LIMIT = 10000
INGEST_TS   = datetime.now(timezone.utc).isoformat()
INGEST_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

def tbl(name):
    return f"{CATALOG}.{SCHEMA}.wikidata_{name}"

def sparql_query(query):
    resp = requests.get(
        SPARQL_URL,
        params={"query": query, "format": "json"},
        headers=HEADERS,
        timeout=55
    )
    resp.raise_for_status()
    return resp.json()["results"]["bindings"]

def paginate_sparql(query_template, parse_fn, name):
    """Paginate a SPARQL query, write JSONL to Volume, return (path, total)."""
    out_path = f"{VOLUME}/{name}_{INGEST_DATE}.jsonl"
    os.makedirs(VOLUME, exist_ok=True)
    offset, total = 0, 0
    with open(out_path, "w") as f:
        while True:
            print(f"  {name}: offset {offset}...")
            try:
                results = sparql_query(query_template % (START_YEAR, BATCH_LIMIT, offset))
            except Exception as e:
                print(f"  [ERROR] {e} — retry in 10s")
                time.sleep(10)
                continue
            if not results:
                break
            for r in results:
                row = parse_fn(r)
                row["load_ts"] = INGEST_TS
                f.write(json.dumps(row) + "\n")
            total += len(results)
            offset += BATCH_LIMIT
            time.sleep(1)
    print(f"  {name}: {total:,} rows fetched")
    return out_path, total

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.wikidata_raw")

print("Config ready.")
print(f"  Catalog : {CATALOG}")
print(f"  Ingest  : {INGEST_DATE}")

# COMMAND ----------

# DBTITLE 1,Helpers
def get_watermark(name):
    """Return MAX(load_ts) from existing table, or None if missing."""
    try:
        row = spark.table(tbl(name)).agg(F.max("load_ts").alias("max_ts")).collect()[0]
        ts = row["max_ts"]
        if ts:
            print(f"  {name}: watermark = {ts}")
            return str(ts)
    except Exception:
        pass
    print(f"  {name}: no watermark found — will fetch all records")
    return None

def merge_into(name, out_path, merge_keys):
    """MERGE incoming JSONL into existing Delta table on merge_keys."""
    incoming = spark.read.json(out_path)
    if incoming.count() == 0:  # ← change here
        print(f"  {name}: no new rows to merge")
        return
    incoming = incoming.withColumn("load_ts", F.to_timestamp(F.col("load_ts")))
    target = DeltaTable.forName(spark, tbl(name))
    condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
    before = spark.table(tbl(name)).count()
    (target.alias("t")
           .merge(incoming.alias("s"), condition)
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())
    after = spark.table(tbl(name)).count()
    print(f"  {name}: {before:,} -> {after:,} (+{after - before:,})")

def overwrite_table(name, out_path):
    """Full overwrite — for tiny tables where re-fetching everything is cheap."""
    df = spark.read.json(out_path)
    if df.count() == 0:  # ← change here
        print(f"  {name}: no rows returned — skipping overwrite")
        return
    df = df.withColumn("load_ts", F.to_timestamp(F.col("load_ts")))
    before = spark.table(tbl(name)).count()
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(tbl(name))
    after = spark.table(tbl(name)).count()
    print(f"  {name}: {before:,} -> {after:,} (full overwrite)")
    
print("Helpers ready.")

# COMMAND ----------

# DBTITLE 1,imdb_ids — watermark MERGE
print("\n=== imdb_ids (watermark MERGE) ===")
watermark = get_watermark("imdb_ids")

if watermark:
    date_filter = f'FILTER(?modified >= "{watermark[:10]}"^^xsd:date)'
    modified_triple = "?film schema:dateModified ?modified ."
else:
    date_filter = ""
    modified_triple = ""

IMDB_ID_Q = f"""
SELECT ?film ?imdbId ?year WHERE {{
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  {modified_triple}
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  {date_filter}
}}
LIMIT %d OFFSET %d
"""

out_path, total = paginate_sparql(
    IMDB_ID_Q,
    lambda r: {
        "wikidata_id": r["film"]["value"].split("/")[-1],
        "imdb_id":     r["imdbId"]["value"],
        "year":        r["year"]["value"],
        "method":      "wikidata_direct",
    },
    "imdb_ids"
)

if total > 0:
    merge_into("imdb_ids", out_path, ["wikidata_id", "imdb_id"])
else:
    print("  imdb_ids: no new/updated records since watermark")

# COMMAND ----------

# DBTITLE 1,based_on — watermark MERGE
print("\n=== based_on (watermark MERGE) ===")
watermark = get_watermark("based_on")

if watermark:
    date_filter = f'FILTER(?modified >= "{watermark[:10]}"^^xsd:date)'
    modified_triple = "?film schema:dateModified ?modified ."
else:
    date_filter = ""
    modified_triple = ""

BASED_ON_Q = f"""
SELECT ?film ?imdbId ?source ?sourceLabel WHERE {{
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  ?film wdt:P144 ?source .
  {modified_triple}
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  {date_filter}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
LIMIT %d OFFSET %d
"""

out_path, total = paginate_sparql(
    BASED_ON_Q,
    lambda r: {
        "wikidata_id":        r["film"]["value"].split("/")[-1],
        "imdb_id":            r["imdbId"]["value"],
        "source_wikidata_id": r["source"]["value"].split("/")[-1],
        "source_label":       r["sourceLabel"]["value"],
        "method":             "wikidata_direct",
    },
    "based_on"
)

if total > 0:
    merge_into("based_on", out_path, ["wikidata_id", "source_wikidata_id"])
else:
    print("  based_on: no new/updated records since watermark")

# COMMAND ----------

# DBTITLE 1,movements — full overwrite
print("\n=== movements (full overwrite) ===")

MOVEMENT_Q = """
SELECT ?film ?imdbId ?movement ?movementLabel WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  ?film wdt:P135 ?movement .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT %d OFFSET %d
"""

out_path, total = paginate_sparql(
    MOVEMENT_Q,
    lambda r: {
        "wikidata_id":          r["film"]["value"].split("/")[-1],
        "imdb_id":              r["imdbId"]["value"],
        "movement_wikidata_id": r["movement"]["value"].split("/")[-1],
        "movement_label":       r["movementLabel"]["value"],
        "method":               "wikidata_direct",
        "confidence":           1.0,
    },
    "movements"
)

if total > 0:
    overwrite_table("movements", out_path)

# COMMAND ----------

# DBTITLE 1,festivals — full overwrite
print("\n=== festivals (full overwrite) ===")

FESTIVAL_Q = """
SELECT ?film ?imdbId ?festival ?festivalLabel WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  ?film wdt:P5072 ?festival .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT %d OFFSET %d
"""

out_path, total = paginate_sparql(
    FESTIVAL_Q,
    lambda r: {
        "wikidata_id":          r["film"]["value"].split("/")[-1],
        "imdb_id":              r["imdbId"]["value"],
        "festival_wikidata_id": r["festival"]["value"].split("/")[-1],
        "festival_label":       r["festivalLabel"]["value"],
        "method":               "wikidata_direct",
    },
    "festivals"
)

if total > 0:
    overwrite_table("festivals", out_path)

# COMMAND ----------

# DBTITLE 1,influenced_by — full overwrite
print("\n=== influenced_by (full overwrite) ===")

INFLUENCED_Q = """
SELECT ?film ?imdbId ?influence ?influenceLabel WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  ?film wdt:P737 ?influence .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT %d OFFSET %d
"""

out_path, total = paginate_sparql(
    INFLUENCED_Q,
    lambda r: {
        "wikidata_id":           r["film"]["value"].split("/")[-1],
        "imdb_id":               r["imdbId"]["value"],
        "influence_wikidata_id": r["influence"]["value"].split("/")[-1],
        "influence_label":       r["influenceLabel"]["value"],
        "method":                "wikidata_direct",
        "confidence":            1.0,
    },
    "influenced_by"
)

if total > 0:
    overwrite_table("influenced_by", out_path)

# COMMAND ----------

# DBTITLE 1,Summary
print(f"\n=== Wikidata Incremental Summary — {INGEST_DATE} ===")
for name in ["imdb_ids", "movements", "festivals", "based_on", "influenced_by"]:
    try:
        count  = spark.table(tbl(name)).count()
        latest = spark.table(tbl(name)).agg(F.max("load_ts")).collect()[0][0]
        print(f"  {tbl(name):<45} {count:>8,} rows  |  last load: {str(latest)[:19]}")
    except Exception as e:
        print(f"  {tbl(name):<45} ERROR: {e}")