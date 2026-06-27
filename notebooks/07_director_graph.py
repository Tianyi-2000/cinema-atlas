# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Director Graph
# MAGIC Builds a 2D director landscape from thematic embeddings.
# MAGIC
# MAGIC Pipeline:
# MAGIC 1. Load director–film associations from `milkmoo.silver.film_crew`
# MAGIC 2. Load pre-computed film embeddings (384-dim from `all-MiniLM-L6-v2`)
# MAGIC 3. Compute a per-director **style vector** = mean of their films' embeddings
# MAGIC 4. UMAP → 2D positions
# MAGIC 5. K-means (k=15) → cluster assignment
# MAGIC 6. Label clusters via Wikidata P135 (film movement) majority vote
# MAGIC 7. Export `director_graph.json` → copy to `cinema_atlas_next/public/`

# COMMAND ----------
import numpy as np
import pandas as pd
import json, re, time, requests
from collections import Counter

# COMMAND ----------
# MAGIC %md ## 1 · Director–film pairs

# COMMAND ----------
crew_raw = spark.sql("""
    SELECT
        fc.person_id,
        fc.name,
        fc.film_id,
        CASE
            WHEN lower(fc.job) = 'director'                  THEN 'Director'
            WHEN lower(fc.job) IN ('director of photography',
                                   'cinematographer')         THEN 'Cinematographer'
        END AS role,
        m.vote_average,
        m.vote_count,
        m.title
    FROM milkmoo.silver.film_crew fc
    JOIN milkmoo.silver.movies m ON fc.film_id = m.id
    WHERE (
        lower(fc.job) = 'director'
        OR lower(fc.job) IN ('director of photography', 'cinematographer')
    )
    AND m.vote_count >= 100
    AND fc.name IS NOT NULL
""").toPandas()

# Alias so the rest of the notebook keeps working
directors_raw = crew_raw

dirs = crew_raw[crew_raw.role=='Director'].person_id.nunique()
dops = crew_raw[crew_raw.role=='Cinematographer'].person_id.nunique()
print(f"Rows: {len(crew_raw):,}  |  Directors: {dirs:,}  |  Cinematographers: {dops:,}")

# COMMAND ----------
# MAGIC %md ## 2 · Film embeddings
# MAGIC
# MAGIC **Option A** (preferred): load from the gold table written by notebook 06.
# MAGIC **Option B** (fallback): re-compute from TMDB overviews.

# COMMAND ----------
# ── Option A ────────────────────────────────────────────────────────────────
try:
    emb_raw = spark.sql("""
        SELECT film_id, embedding
        FROM workspace.gold.film_embeddings
    """).toPandas()

    def _parse(e):
        if isinstance(e, str):   return np.array(json.loads(e), dtype=np.float32)
        if hasattr(e, '__iter__'): return np.array(list(e), dtype=np.float32)
        return None

    emb_raw['vec'] = emb_raw['embedding'].apply(_parse)
    emb_raw = emb_raw.dropna(subset=['vec'])
    emb_map = {int(r.film_id): r.vec for r in emb_raw.itertuples()}
    dim = next(iter(emb_map.values())).shape[0]
    print(f"Loaded {len(emb_map):,} embeddings from gold table  (dim={dim})")

except Exception as e:
    print(f"Gold table unavailable ({e}), falling back to re-computing embeddings…")

    # ── Option B ─────────────────────────────────────────────────────────────
    from sentence_transformers import SentenceTransformer

    films_needed = set(directors_raw.film_id.unique())
    overviews_df = spark.sql(f"""
        SELECT id AS film_id, overview
        FROM milkmoo.silver.movies
        WHERE id IN ({','.join(map(str, films_needed))})
          AND overview IS NOT NULL AND length(overview) > 30
    """).toPandas()

    model = SentenceTransformer('all-MiniLM-L6-v2')
    vecs  = model.encode(overviews_df['overview'].tolist(),
                         batch_size=256, show_progress_bar=True,
                         normalize_embeddings=True)
    emb_map = dict(zip(overviews_df['film_id'].astype(int), vecs))
    print(f"Computed {len(emb_map):,} embeddings  (dim={vecs.shape[1]})")

# COMMAND ----------
# MAGIC %md ## 3 · Director style vectors

# COMMAND ----------
person_data = {}
for _, row in directors_raw.iterrows():
    pid = int(row['person_id'])
    fid = int(row['film_id'])
    if fid not in emb_map:
        continue
    if pid not in person_data:
        person_data[pid] = {
            'name':      row['name'],
            'role':      row['role'],
            'films':     [],
            'vec_sum':   np.zeros_like(next(iter(emb_map.values()))),
            'vec_count': 0,
        }
    person_data[pid]['vec_sum']   += emb_map[fid]
    person_data[pid]['vec_count'] += 1
    person_data[pid]['films'].append({
        'film_id':      fid,
        'title':        row['title'],
        'vote_average': float(row['vote_average']),
        'vote_count':   int(row['vote_count']),
    })

# Directors need ≥3 films (more prolific); cinematographers ≥2 (often shoot fewer)
def min_films(role): return 3 if role == 'Director' else 2

director_list = []
for pid, d in person_data.items():
    if d['vec_count'] < min_films(d['role']):
        continue
    vec = d['vec_sum'] / d['vec_count']
    vec = vec / (np.linalg.norm(vec) + 1e-9)
    d['films'].sort(key=lambda f: -f['vote_average'])
    director_list.append({
        'person_id':      pid,
        'name':           d['name'],
        'role':           d['role'],
        'film_count':     d['vec_count'],
        'vec':            vec,
        'top_film_id':    d['films'][0]['film_id']  if d['films'] else None,
        'top_film_title': d['films'][0]['title']    if d['films'] else '',
    })

dirs = sum(1 for d in director_list if d['role']=='Director')
dops = sum(1 for d in director_list if d['role']=='Cinematographer')
print(f"Directors: {dirs:,}  |  Cinematographers: {dops:,}  |  Total: {len(director_list):,}")

# COMMAND ----------
# MAGIC %md ## 4 · UMAP → 2D positions

# COMMAND ----------
import umap.umap_ as umap_lib

vecs_matrix = np.stack([d['vec'] for d in director_list])
reducer = umap_lib.UMAP(
    n_components=2,
    n_neighbors=15,
    min_dist=0.1,
    metric='cosine',
    random_state=42,
)
coords = reducer.fit_transform(vecs_matrix)

# Normalise to [0, 1]
coords -= coords.min(axis=0)
coords /= coords.max(axis=0)

for i, d in enumerate(director_list):
    d['x'] = float(coords[i, 0])
    d['y'] = float(coords[i, 1])

print("UMAP done")

# COMMAND ----------
# MAGIC %md ## 5 · K-means clustering

# COMMAND ----------
from sklearn.cluster import KMeans

K = 15
km = KMeans(n_clusters=K, random_state=42, n_init=10)
cluster_ids = km.fit_predict(coords)

for i, d in enumerate(director_list):
    d['cluster'] = int(cluster_ids[i])

print(f"K-means done  (k={K})")
for c in range(K):
    members = [d['name'] for d in director_list if d['cluster']==c]
    print(f"  Cluster {c:2d}: {len(members):3d} directors — {', '.join(members[:4])}")

# COMMAND ----------
# MAGIC %md ## 6 · Wikidata P135 labels (film movement)
# MAGIC
# MAGIC Queries Wikidata using each director's TMDb person ID (Wikidata property P4985).
# MAGIC Coverage is ~30-50% for well-known directors; clusters with no coverage fall back
# MAGIC to genre-based or name-based labels.

# COMMAND ----------
def _wikidata_batch(tmdb_ids):
    """Return {tmdb_id_str: movement_label} for a batch of ≤50 IDs."""
    vals   = ' '.join(f'"{i}"' for i in tmdb_ids)
    sparql = f"""
    SELECT ?tmdb_id ?movementLabel WHERE {{
      VALUES ?tmdb_id {{ {vals} }}
      ?person wdt:P4985 ?tmdb_id .
      ?person wdt:P135  ?movement .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    """
    try:
        r = requests.get(
            'https://query.wikidata.org/sparql',
            params={'query': sparql, 'format': 'json'},
            headers={'User-Agent': 'CinemaAtlas/1.0 (research project)'},
            timeout=30,
        )
        if r.status_code == 200:
            rows = r.json()['results']['bindings']
            return {row['tmdb_id']['value']: row['movementLabel']['value'] for row in rows}
    except Exception as ex:
        print(f"  Wikidata error: {ex}")
    return {}

movement_map = {}
all_ids = [str(d['person_id']) for d in director_list]
BATCH = 50
for i in range(0, len(all_ids), BATCH):
    batch   = all_ids[i:i+BATCH]
    result  = _wikidata_batch(batch)
    movement_map.update(result)
    time.sleep(1)
    if (i // BATCH) % 5 == 0:
        print(f"  {min(i+BATCH, len(all_ids))}/{len(all_ids)} queried…")

for d in director_list:
    d['movement'] = movement_map.get(str(d['person_id']))

covered = sum(1 for d in director_list if d['movement'])
print(f"Wikidata movement coverage: {covered}/{len(director_list)} ({100*covered/len(director_list):.1f}%)")

# COMMAND ----------
# MAGIC %md ## 7 · Cluster labeling
# MAGIC
# MAGIC Priority: (1) Wikidata P135 majority vote → (2) dominant genre → (3) top director names.

# COMMAND ----------
# Fetch dominant genre per cluster from TMDB
film_ids_all = list({f['film_id'] for d in director_list for f in []})  # rebuilt below

# Reconstruct film→cluster mapping through directors
film_cluster = {}
for d in director_list:
    # We don't store films on d after the vec step, but we kept top_film_id
    # Use directors_raw to recover all film_ids per director
    pass

# Simpler: map cluster → genres via directors_raw + milkmoo genre table
dir_cluster = {d['person_id']: d['cluster'] for d in director_list}

genre_rows = spark.sql("""
    SELECT fc.person_id, mg.genre_name
    FROM milkmoo.silver.film_crew fc
    JOIN milkmoo.silver.movie_genres mg ON fc.film_id = mg.movie_id
    WHERE lower(fc.job) = 'director'
""").toPandas()

cluster_genres  = {i: [] for i in range(K)}
for _, row in genre_rows.iterrows():
    pid = int(row['person_id'])
    if pid in dir_cluster:
        cluster_genres[dir_cluster[pid]].append(row['genre_name'])

# Build final cluster labels
cluster_label_map = {}
for c in range(K):
    # (1) Wikidata P135 majority
    movements = [d['movement'] for d in director_list if d['cluster']==c and d['movement']]
    if len(movements) >= 3:
        cluster_label_map[c] = Counter(movements).most_common(1)[0][0]
        continue

    # (2) Dominant genre combo
    genres = cluster_genres.get(c, [])
    if genres:
        top2 = [g for g, _ in Counter(genres).most_common(2)]
        cluster_label_map[c] = ' & '.join(top2)
        continue

    # (3) Top director names
    top_dirs = sorted([d for d in director_list if d['cluster']==c],
                      key=lambda d: -d['film_count'])[:3]
    cluster_label_map[c] = ', '.join(d['name'] for d in top_dirs)

print("\nCluster labels:")
for c in range(K):
    members = [d['name'] for d in director_list if d['cluster']==c]
    print(f"  {c:2d}  {cluster_label_map[c]:<40s}  ({len(members)} dirs — {', '.join(members[:3])})")

# COMMAND ----------
# MAGIC %md ## 8 · Export JSON

# COMMAND ----------
output_nodes = [{
    'person_id':      d['person_id'],
    'name':           d['name'],
    'role':           d['role'],
    'film_count':     d['film_count'],
    'x':              round(d['x'], 5),
    'y':              round(d['y'], 5),
    'cluster':        d['cluster'],
    'cluster_label':  cluster_label_map[d['cluster']],
    'movement':       d.get('movement'),
    'top_film_id':    d['top_film_id'],
    'top_film_title': d['top_film_title'],
} for d in director_list]

json_str = json.dumps({'nodes': output_nodes}, ensure_ascii=False)

# Write to DBFS
out_path = '/FileStore/cinema_atlas/director_graph.json'
dbutils.fs.put(out_path, json_str, overwrite=True)
print(f"Wrote {len(output_nodes):,} director nodes → dbfs:{out_path}")
print(f"\nDownload URL (replace <workspace>):")
print(f"  https://<workspace>.azuredatabricks.net/files/cinema_atlas/director_graph.json")
print(f"\nThen copy to:  cinema_atlas_next/public/director_graph.json")
