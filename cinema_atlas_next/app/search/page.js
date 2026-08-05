"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"

const IMG = "https://image.tmdb.org/t/p/w342"

const EXAMPLES = [
  "funny but also emotional",
  "a slow-burn sci-fi about space",
  "feel-good sports movie",
  "a mind-bending thriller",
  "cozy movie for a rainy day",
]

export default function DiscoverPage() {
  const [term, setTerm] = useState("")
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const [searched, setSearched] = useState(false)
  const router = useRouter()

  async function runSearch(query) {
    if (!query.trim()) return
    setLoading(true)
    setSearched(true)
    try {
      const res = await fetch(`/api/discover?q=${encodeURIComponent(query)}`)
      const data = await res.json()
      setResults(data.results || [])
    } catch {
      setResults([])
    }
    setLoading(false)
  }

  function handleSubmit(e) {
    e.preventDefault()
    runSearch(term)
  }

  function handleExample(ex) {
    setTerm(ex)
    runSearch(ex)
  }

  return (
    <div>
      {/* hero */}
      <div style={{ marginBottom: "2rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "0.5rem" }}>
          <div style={{ width: 26, height: 2, background: "#E8B14C" }} />
          <span style={{ color: "#E8B14C", fontSize: "0.72rem", fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase" }}>
            AI discovery
          </span>
        </div>
        <h1 style={{ margin: 0, fontSize: "2rem" }}>Describe a movie, not a title</h1>
        <p style={{ color: "#8A8779", marginTop: "0.6rem", fontSize: "1rem", maxWidth: "36rem" }}>
          Tell us the mood, theme, or feeling you&apos;re after — in your own words — and we&apos;ll find the closest matches across the catalog.
        </p>
      </div>

      {/* hero search bar */}
      <form onSubmit={handleSubmit} style={{ display: "flex", gap: "0.75rem", marginBottom: "1rem" }}>
        <input
          value={term}
          onChange={e => setTerm(e.target.value)}
          placeholder="e.g. a heartwarming film about second chances…"
          style={{
            flex: 1, background: "#16161C", border: "1px solid #24242c",
            borderRadius: 10, padding: "1rem 1.25rem", color: "#EDE7DA",
            fontSize: "1.05rem", outline: "none",
          }}
        />
        <button type="submit" disabled={loading} style={{
          background: "#E8B14C", color: "#0E0E12", border: "none",
          borderRadius: 10, padding: "1rem 1.75rem", fontWeight: 600,
          cursor: loading ? "default" : "pointer", fontSize: "1rem",
          opacity: loading ? 0.6 : 1,
        }}>
          {loading ? "Searching…" : "Discover"}
        </button>
      </form>

      {/* example chips */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", marginBottom: "2.5rem" }}>
        {EXAMPLES.map(ex => (
          <button
            key={ex}
            onClick={() => handleExample(ex)}
            style={{
              background: "transparent", border: "1px solid #24242c",
              borderRadius: 999, padding: "0.4rem 0.9rem", color: "#8A8779",
              fontSize: "0.82rem", cursor: "pointer", transition: "all 0.2s",
            }}
            onMouseEnter={e => { e.currentTarget.style.borderColor = "#E8B14C"; e.currentTarget.style.color = "#EDE7DA" }}
            onMouseLeave={e => { e.currentTarget.style.borderColor = "#24242c"; e.currentTarget.style.color = "#8A8779" }}
          >
            {ex}
          </button>
        ))}
      </div>

      {/* loading */}
      {loading && (
        <p style={{ color: "#8A8779" }}>
          Reading the catalog and matching your description… the first search can take a few seconds.
        </p>
      )}

      {/* empty state after a search with no results */}
      {!loading && searched && results.length === 0 && (
        <p style={{ color: "#8A8779" }}>
          No close matches. Try describing the mood or theme a little differently.
        </p>
      )}

      {/* results */}
      {!loading && results.length > 0 && (
        <>
          <p style={{ color: "#8A8779", marginBottom: "1rem" }}>
            Closest matches for “{term}”
          </p>
          <div style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))",
            gap: "1rem",
          }}>
            {results.map(film => (
              <div
                key={film.film_id}
                onClick={() => router.push(`/film/${film.film_id}`)}
                style={{ cursor: "pointer" }}
              >
                <div style={{
                  background: "#16161C", borderRadius: 8,
                  border: "1px solid #24242c", overflow: "hidden",
                  position: "relative",
                }}>
                  {/* relevance badge */}
                  {film.score != null && (
                    <div style={{
                      position: "absolute", top: 8, right: 8, zIndex: 1,
                      background: "rgba(14,14,18,0.82)", color: "#E8B14C",
                      fontSize: "0.68rem", fontWeight: 600, padding: "0.2rem 0.45rem",
                      borderRadius: 5, backdropFilter: "blur(4px)",
                    }}>
                      {Math.round(film.score * 100)}% match
                    </div>
                  )}
                  {film.poster_path ? (
                    <img
                      src={`${IMG}${film.poster_path}`}
                      alt={film.title}
                      style={{ width: "100%", display: "block" }}
                    />
                  ) : (
                    <div style={{
                      aspectRatio: "2/3", background: "#24242c",
                      display: "flex", alignItems: "center",
                      justifyContent: "center", color: "#8A8779",
                      fontSize: "0.8rem",
                    }}>
                      No poster
                    </div>
                  )}
                  <div style={{ padding: "0.6rem 0.7rem" }}>
                    <div style={{ fontSize: "0.85rem", fontWeight: 600, color: "#EDE7DA" }}>
                      {film.title}
                    </div>
                    <div style={{ fontSize: "0.75rem", color: "#8A8779", marginTop: "0.2rem" }}>
                      {film.release_date?.slice(0, 4)}
                      {Number(film.vote_average) > 0 && <> · ★ {Number(film.vote_average).toFixed(1)}</>}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}