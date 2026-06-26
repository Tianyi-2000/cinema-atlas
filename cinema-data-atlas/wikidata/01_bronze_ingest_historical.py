# Databricks notebook source
# Configuration
import requests, json, time, os
from datetime import date

CATALOG = "workspace"
SCHEMA  = "bronze"
VOLUME  = "/Volumes/workspace/bronze/wikidata_raw"

SPARQL_URL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "TruffleBlanche/1.0 (USF MSDS683 cinema KG project; contact: aatish.lobo@gmail.com)",
    "Accept": "application/json"
}
START_YEAR  = 2000
BATCH_LIMIT = 10000
INGEST_DATE = str(date.today())

def tbl(name): return f"{CATALOG}.{SCHEMA}.wikidata_{name}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.wikidata_raw")

def sparql_query(query):
    resp = requests.get(SPARQL_URL, params={"query": query, "format": "json"},
                        headers=HEADERS, timeout=55)
    resp.raise_for_status()
    return resp.json()["results"]["bindings"]

def ingest_property(query_template, parse_fn, name):
    """Paginate a SPARQL query, write all rows to a Volume jsonl, load to Delta."""
    out_path = f"{VOLUME}/{name}_{INGEST_DATE}.jsonl"
    offset, total = 0, 0
    with open(out_path, "w") as f:
        while True:
            print(f"  {name}: offset {offset}...")
            try:
                results = sparql_query(query_template % (START_YEAR, BATCH_LIMIT, offset))
            except Exception as e:
                print(f"  [ERROR] {e} — retry in 10s")
                time.sleep(10); continue
            if not results:
                break
            for r in results:
                f.write(json.dumps(parse_fn(r)) + "\n")
            total += len(results)
            offset += BATCH_LIMIT
            time.sleep(1)
    if total == 0:
        print(f"  {name}: no rows.")
        return
    spark.read.json(out_path).write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(tbl(name))
    print(f"  {name}: {total:,} rows -> {tbl(name)}")

print("Config ready.")

# COMMAND ----------

IMDB_ID_Q = """
SELECT ?film ?imdbId ?year WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
}
LIMIT %d OFFSET %d
"""
ingest_property(IMDB_ID_Q, lambda r: {
    "wikidata_id": r["film"]["value"].split("/")[-1],
    "imdb_id": r["imdbId"]["value"],
    "year": r["year"]["value"],
    "method": "wikidata_direct",
}, "imdb_ids")

# COMMAND ----------

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
ingest_property(MOVEMENT_Q, lambda r: {
    "wikidata_id": r["film"]["value"].split("/")[-1],
    "imdb_id": r["imdbId"]["value"],
    "movement_wikidata_id": r["movement"]["value"].split("/")[-1],
    "movement_label": r["movementLabel"]["value"],
    "method": "wikidata_direct",
    "confidence": 1.0,
}, "movements")

# COMMAND ----------

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
ingest_property(FESTIVAL_Q, lambda r: {
    "wikidata_id": r["film"]["value"].split("/")[-1],
    "imdb_id": r["imdbId"]["value"],
    "festival_wikidata_id": r["festival"]["value"].split("/")[-1],
    "festival_label": r["festivalLabel"]["value"],
    "method": "wikidata_direct",
}, "festivals")

# COMMAND ----------

BASED_ON_Q = """
SELECT ?film ?imdbId ?source ?sourceLabel WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  ?film wdt:P144 ?source .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT %d OFFSET %d
"""
ingest_property(BASED_ON_Q, lambda r: {
    "wikidata_id": r["film"]["value"].split("/")[-1],
    "imdb_id": r["imdbId"]["value"],
    "source_wikidata_id": r["source"]["value"].split("/")[-1],
    "source_label": r["sourceLabel"]["value"],
    "method": "wikidata_direct",
}, "based_on")

# COMMAND ----------

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
ingest_property(INFLUENCED_Q, lambda r: {
    "wikidata_id": r["film"]["value"].split("/")[-1],
    "imdb_id": r["imdbId"]["value"],
    "influence_wikidata_id": r["influence"]["value"].split("/")[-1],
    "influence_label": r["influenceLabel"]["value"],
    "method": "wikidata_direct",
    "confidence": 1.0,
}, "influenced_by")

# COMMAND ----------

print("=== Wikidata Bronze Summary ===")
for name in ["imdb_ids", "movements", "festivals", "based_on", "influenced_by"]:
    try:
        print(f"  {tbl(name):<40} {spark.table(tbl(name)).count():>7,} rows")
    except Exception:
        print(f"  {tbl(name):<40} (not created — no rows)")

# COMMAND ----------

movements = spark.table("workspace.bronze.wikidata_movements")
matched = spark.table("workspace.silver.matched_tconsts")

movements.join(matched, movements.imdb_id == matched.tconst, "inner").count()

# COMMAND ----------

matched = spark.table("workspace.silver.matched_tconsts")

based = spark.table("workspace.bronze.wikidata_based_on")
fest  = spark.table("workspace.bronze.wikidata_festivals")
infl  = spark.table("workspace.bronze.wikidata_influenced_by")

print("based_on in matched: ", based.join(matched, based.imdb_id == matched.tconst, "inner").count())
print("festivals in matched:", fest.join(matched, fest.imdb_id == matched.tconst, "inner").count())
print("influenced in matched:", infl.join(matched, infl.imdb_id == matched.tconst, "inner").count())