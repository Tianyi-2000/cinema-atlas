# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Thematic Embedding Map (TMDB overview)
# MAGIC ## Films → plot embeddings → kNN thematic edges → organic blended landscape
# MAGIC
# MAGIC Embeds each film's plot `overview` with a sentence transformer, then connects each film to its *k* nearest thematic neighbors. No hard clustering — hybrid films bridge thematic regions, knitting the graph into one continuous blended fabric.
# MAGIC
# MAGIC **Inputs:** `workspace.silver.films` (has `overview`).
# MAGIC **Outputs:**
# MAGIC - `workspace.gold.film_embeddings` — film_id, 2D layout coords, embedding-derived color
# MAGIC - `workspace.gold.thematic_edges` — kNN edge list (src_film, dst_film, similarity)
# MAGIC
# MAGIC Start with TMDB overview (already in our data). If thematic gradients feel coarse, swap in Wikipedia extracts later — only the text source changes, the pipeline stays.

# COMMAND ----------

# MAGIC %pip install sentence-transformers scikit-learn umap-learn
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
import numpy as np

CATALOG = "workspace"
OUT_SCHEMA = "gold"
def out(name): return f"{CATALOG}.{OUT_SCHEMA}.{name}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OUT_SCHEMA}")

K_NEIGHBORS = 12   # higher = more blending / connective tissue

# load films with a usable overview
films = (
    spark.table("workspace.silver.films")
    .select("id", "title", "year", "overview")
    .filter(F.col("overview").isNotNull() & (F.length("overview") > 20))
).toPandas()

print(f"Films with usable overview: {len(films):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — embed plot overviews
# MAGIC `all-MiniLM-L6-v2`, 384-dim. Batched. Films with similar plots land near each other in embedding space.

# COMMAND ----------

from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
texts = films["overview"].tolist()

embeddings = model.encode(
    texts, batch_size=64, show_progress_bar=True, normalize_embeddings=True
)
print(f"Embeddings: {embeddings.shape}")   # (N, 384)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — kNN thematic edges
# MAGIC Each film links to its K nearest neighbors by cosine similarity. Bridge films (hybrids) connect to multiple thematic regions — that's the connective tissue that blends the graph. No threshold cutoff, so even cross-region bridges survive.

# COMMAND ----------

from sklearn.neighbors import NearestNeighbors

# cosine == euclidean on normalized vectors; use brute for exactness at this scale
nn = NearestNeighbors(n_neighbors=K_NEIGHBORS + 1, metric="cosine", algorithm="brute")
nn.fit(embeddings)
distances, indices = nn.kneighbors(embeddings)

film_ids = films["id"].tolist()
edges = []
for i in range(len(film_ids)):
    for j_pos in range(1, K_NEIGHBORS + 1):   # skip self (col 0)
        j = indices[i][j_pos]
        sim = 1.0 - distances[i][j_pos]
        edges.append((int(film_ids[i]), int(film_ids[j]), float(sim)))

print(f"kNN edges: {len(edges):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — 2D layout + positional color
# MAGIC UMAP projects the 384-dim space to 2D for the visual landscape. The 2D position also maps to a continuous color, so films blend in color the way they blend in theme — no hard cluster boundaries.

# COMMAND ----------

import umap

reducer = umap.UMAP(n_neighbors=15, min_dist=0.1, metric="cosine", random_state=42)
coords = reducer.fit_transform(embeddings)   # (N, 2)

# normalize coords to 0-1 for layout + color mapping
cx = (coords[:, 0] - coords[:, 0].min()) / (coords[:, 0].ptp())
cy = (coords[:, 1] - coords[:, 1].min()) / (coords[:, 1].ptp())

# positional color: map (x,y) -> hue/lightness so nearby films share color
import colorsys
def pos_to_hex(x, y):
    h = x                      # hue from x
    s = 0.55
    l = 0.35 + 0.30 * y        # lightness from y
    r, g, b = colorsys.hls_to_rgb(h, l, s)
    return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"

colors = [pos_to_hex(cx[i], cy[i]) for i in range(len(cx))]
print("Layout + colors computed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — write Gold tables

# COMMAND ----------

import pandas as pd

nodes_pdf = pd.DataFrame({
    "film_id": film_ids,
    "title": films["title"].tolist(),
    "year": films["year"].tolist(),
    "x": cx.tolist(),
    "y": cy.tolist(),
    "color": colors,
})
spark.createDataFrame(nodes_pdf).write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(out("film_embeddings"))

edges_pdf = pd.DataFrame(edges, columns=["src_film", "dst_film", "similarity"])
spark.createDataFrame(edges_pdf).write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(out("thematic_edges"))

print(f"Wrote {out('film_embeddings')}: {len(nodes_pdf):,} nodes")
print(f"Wrote {out('thematic_edges')}:  {len(edges_pdf):,} edges")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — eyeball the thematic neighborhoods
# MAGIC Pick a film, show its nearest thematic neighbors. If they feel thematically related, the embedding is working.

# COMMAND ----------

# helper: show a film's nearest neighbors by title
id_to_title = dict(zip(film_ids, films["title"].tolist()))

def neighbors_of(title_substr, n=10):
    matches = [i for i, t in enumerate(films["title"].tolist())
               if title_substr.lower() in str(t).lower()]
    if not matches:
        print("no match"); return
    i = matches[0]
    print(f"Nearest thematic neighbors of '{films['title'].iloc[i]}':")
    for j_pos in range(1, n + 1):
        j = indices[i][j_pos]
        sim = 1.0 - distances[i][j_pos]
        print(f"  {sim:.3f}  {films['title'].iloc[j]} ({films['year'].iloc[j]})")

# try a few — swap in titles you know
neighbors_of("Memento")
print()
neighbors_of("Mad Max")