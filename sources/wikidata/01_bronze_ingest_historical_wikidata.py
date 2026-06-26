# Databricks notebook source
import requests
import json
import time
import boto3

# AWS credentials

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

SPARQL_URL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "TruffleBlanche/1.0 (cinema knowledge graph project; contact: your_email@usfca.edu)",
    "Accept": "application/json"
}
START_YEAR = 2000
BATCH_LIMIT = 10000

def sparql_query(query):
    resp = requests.get(
        SPARQL_URL,
        params={"query": query, "format": "json"},
        headers=HEADERS,
        timeout=55
    )
    resp.raise_for_status()
    return resp.json()["results"]["bindings"]

def write_batch(rows, prefix, batch_num):
    payload = "\n".join(json.dumps(row) for row in rows)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}batch_{batch_num:04d}.jsonl",
        Body=payload,
        ContentType="application/x-ndjson"
    )

print("Config ready.")

# COMMAND ----------

IMDB_ID_QUERY = """
SELECT ?film ?imdbId ?year WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
}
LIMIT %d
OFFSET %d
"""

PREFIX = "bronze/wikidata/historical/imdb_ids/"

offset = 0
batch_num = 0
total = 0

while True:
    print(f"  Fetching offset {offset}...")
    try:
        results = sparql_query(IMDB_ID_QUERY % (START_YEAR, BATCH_LIMIT, offset))
    except Exception as e:
        print(f"  [ERROR] {e} — retrying in 10s")
        time.sleep(10)
        continue

    if not results:
        break

    rows = []
    for r in results:
        rows.append({
            "wikidata_id": r["film"]["value"].split("/")[-1],
            "imdb_id": r["imdbId"]["value"],
            "year": r["year"]["value"],
            "method": "wikidata_direct"
        })

    write_batch(rows, PREFIX, batch_num)
    total += len(rows)
    print(f"  batch {batch_num:04d} written ({total} rows so far)")
    batch_num += 1
    offset += BATCH_LIMIT
    time.sleep(1)

print(f"\nDone — {total} Wikidata films with IMDb IDs (year >= {START_YEAR}).")

# COMMAND ----------

MOVEMENT_QUERY = """
SELECT ?film ?imdbId ?movement ?movementLabel WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  ?film wdt:P135 ?movement .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT %d
OFFSET %d
"""

PREFIX = "bronze/wikidata/historical/movements/"

offset = 0
batch_num = 0
total = 0

while True:
    print(f"  Fetching offset {offset}...")
    try:
        results = sparql_query(MOVEMENT_QUERY % (START_YEAR, BATCH_LIMIT, offset))
    except Exception as e:
        print(f"  [ERROR] {e} — retrying in 10s")
        time.sleep(10)
        continue

    if not results:
        break

    rows = []
    for r in results:
        rows.append({
            "wikidata_id": r["film"]["value"].split("/")[-1],
            "imdb_id": r["imdbId"]["value"],
            "movement_wikidata_id": r["movement"]["value"].split("/")[-1],
            "movement_label": r["movementLabel"]["value"],
            "method": "wikidata_direct",
            "confidence": 1.0
        })

    write_batch(rows, PREFIX, batch_num)
    total += len(rows)
    print(f"  batch {batch_num:04d} written ({total} rows so far)")
    batch_num += 1
    offset += BATCH_LIMIT
    time.sleep(1)

print(f"\nDone — {total} movement-tagged films (year >= {START_YEAR}).")
print("Note: low count is expected — P135 coverage is sparse. This is the seed set for inference, not the final table.")

# COMMAND ----------

FESTIVAL_QUERY = """
SELECT ?film ?imdbId ?festival ?festivalLabel WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  ?film wdt:P5072 ?festival .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT %d
OFFSET %d
"""

PREFIX = "bronze/wikidata/historical/festivals/"

offset = 0
batch_num = 0
total = 0

while True:
    print(f"  Fetching offset {offset}...")
    try:
        results = sparql_query(FESTIVAL_QUERY % (START_YEAR, BATCH_LIMIT, offset))
    except Exception as e:
        print(f"  [ERROR] {e} — retrying in 10s")
        time.sleep(10)
        continue

    if not results:
        break

    rows = []
    for r in results:
        rows.append({
            "wikidata_id": r["film"]["value"].split("/")[-1],
            "imdb_id": r["imdbId"]["value"],
            "festival_wikidata_id": r["festival"]["value"].split("/")[-1],
            "festival_label": r["festivalLabel"]["value"],
            "method": "wikidata_direct"
        })

    write_batch(rows, PREFIX, batch_num)
    total += len(rows)
    print(f"  batch {batch_num:04d} written ({total} rows so far)")
    batch_num += 1
    offset += BATCH_LIMIT
    time.sleep(1)

print(f"\nDone — {total} festival-tagged films (year >= {START_YEAR}).")

# COMMAND ----------

BASED_ON_QUERY = """
SELECT ?film ?imdbId ?source ?sourceLabel WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  ?film wdt:P144 ?source .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT %d
OFFSET %d
"""

PREFIX = "bronze/wikidata/historical/based_on/"

offset = 0
batch_num = 0
total = 0

while True:
    print(f"  Fetching offset {offset}...")
    try:
        results = sparql_query(BASED_ON_QUERY % (START_YEAR, BATCH_LIMIT, offset))
    except Exception as e:
        print(f"  [ERROR] {e} — retrying in 10s")
        time.sleep(10)
        continue

    if not results:
        break

    rows = []
    for r in results:
        rows.append({
            "wikidata_id": r["film"]["value"].split("/")[-1],
            "imdb_id": r["imdbId"]["value"],
            "source_wikidata_id": r["source"]["value"].split("/")[-1],
            "source_label": r["sourceLabel"]["value"],
            "method": "wikidata_direct"
        })

    write_batch(rows, PREFIX, batch_num)
    total += len(rows)
    print(f"  batch {batch_num:04d} written ({total} rows so far)")
    batch_num += 1
    offset += BATCH_LIMIT
    time.sleep(1)

print(f"\nDone — {total} based-on relationships (year >= {START_YEAR}).")

# COMMAND ----------

INFLUENCED_BY_QUERY = """
SELECT ?film ?imdbId ?influence ?influenceLabel WHERE {
  ?film wdt:P31 wd:Q11424 .
  ?film wdt:P345 ?imdbId .
  ?film wdt:P577 ?date .
  ?film wdt:P737 ?influence .
  BIND(YEAR(?date) AS ?year)
  FILTER(?year >= %d)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT %d
OFFSET %d
"""

PREFIX = "bronze/wikidata/historical/influenced_by/"

offset = 0
batch_num = 0
total = 0

while True:
    print(f"  Fetching offset {offset}...")
    try:
        results = sparql_query(INFLUENCED_BY_QUERY % (START_YEAR, BATCH_LIMIT, offset))
    except Exception as e:
        print(f"  [ERROR] {e} — retrying in 10s")
        time.sleep(10)
        continue

    if not results:
        break

    rows = []
    for r in results:
        rows.append({
            "wikidata_id": r["film"]["value"].split("/")[-1],
            "imdb_id": r["imdbId"]["value"],
            "influence_wikidata_id": r["influence"]["value"].split("/")[-1],
            "influence_label": r["influenceLabel"]["value"],
            "method": "wikidata_direct",
            "confidence": 1.0
        })

    write_batch(rows, PREFIX, batch_num)
    total += len(rows)
    print(f"  batch {batch_num:04d} written ({total} rows so far)")
    batch_num += 1
    offset += BATCH_LIMIT
    time.sleep(1)

print(f"\nDone — {total} influence relationships (year >= {START_YEAR}).")
print("Note: expect a very low count — P737 is the sparsest property queried here.")

# COMMAND ----------

prefixes = [
    "bronze/wikidata/historical/imdb_ids/",
    "bronze/wikidata/historical/movements/",
    "bronze/wikidata/historical/festivals/",
    "bronze/wikidata/historical/based_on/",
    "bronze/wikidata/historical/influenced_by/",
]

print("=== Wikidata Historical Ingestion Summary ===")
for prefix in prefixes:
    paginator = s3.get_paginator("list_objects_v2")
    count = sum(
        page.get("KeyCount", 0)
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
    )
    print(f"  {prefix:<55} {count:>7} files")