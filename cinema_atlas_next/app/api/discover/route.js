import { queryDatabricks, queryDatabricksLarge } from "@/lib/databricks"

const HF_MODEL = "BAAI/bge-large-en-v1.5"
const HF_API_URL = `https://router.huggingface.co/hf-inference/models/${HF_MODEL}/pipeline/feature-extraction`
const BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
const CACHE_TTL_MS = 60 * 60 * 1000 // 1 hour

// module-level cache — persists across requests on a warm serverless instance
let cache = { vectors: null, filmIds: null, loadedAt: 0 }

async function loadEmbeddings() {
  const now = Date.now()
  if (cache.vectors && now - cache.loadedAt < CACHE_TTL_MS) {
    return cache
  }

  const rows = await queryDatabricksLarge(`
    SELECT film_id, embedding
    FROM workspace.gold.film_embeddings_v2
  `)

  cache = {
    filmIds: rows.map(r => r.film_id),
    // embedding comes back as a JSON string or array of strings — coerce to floats
    vectors: rows.map(r => {
      const emb = typeof r.embedding === "string" ? JSON.parse(r.embedding) : r.embedding
      return emb.map(Number)
    }),
    loadedAt: now,
  }
  return cache
}

async function embedQuery(query) {
  const res = await fetch(HF_API_URL, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${process.env.HF_API_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ inputs: BGE_QUERY_PREFIX + query }),
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`HF API error (${res.status}): ${text}`)
  }

  const result = await res.json()
  const vec = Array.isArray(result[0]) ? result[0] : result
  return vec.map(Number)
}

function cosineSim(a, b) {
  let dot = 0
  for (let i = 0; i < a.length; i++) dot += a[i] * b[i]
  return dot // vectors are already normalized (normalize_embeddings=True at embed time), so dot product == cosine similarity
}

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const q = searchParams.get("q")

  if (!q || !q.trim()) {
    return Response.json({ error: "Missing query parameter 'q'" }, { status: 400 })
  }

  try {
    const [{ vectors, filmIds }, queryVec] = await Promise.all([
      loadEmbeddings(),
      embedQuery(q),
    ])

    const scored = filmIds.map((filmId, i) => ({
      film_id: filmId,
      score: cosineSim(queryVec, vectors[i]),
    }))

    scored.sort((a, b) => b.score - a.score)
    const top = scored.slice(0, 20)
    const topIds = top.map(t => t.film_id)

    // hydrate display fields from silver
    const filmRows = await queryDatabricks(`
      SELECT film_id, title, release_date, vote_average, poster_path, genres
      FROM workspace.silver_cinema_atlas.tmdb_films
      WHERE film_id IN (${topIds.join(",")})
    `)
    const filmMap = Object.fromEntries(filmRows.map(f => [f.film_id, f]))

    const results = top
      .map(t => ({ ...filmMap[t.film_id], score: t.score }))
      .filter(f => f.title) // drop any film_id that didn't hydrate (shouldn't happen, but safe)

    return Response.json({ query: q, results })
  } catch (err) {
    console.error("discover API error:", err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}