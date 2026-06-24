"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"

const IMG = "https://image.tmdb.org/t/p/w342"

export default function SearchPage() {
  const [term, setTerm] = useState("")
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const router = useRouter()

  async function handleSearch(e) {
    e.preventDefault()
    if (!term.trim()) return
    setLoading(true)
    const res = await fetch(`/api/search?q=${encodeURIComponent(term)}`)
    setResults(await res.json())
    setLoading(false)
  }

  return (
    <div>
      <div style={{ marginBottom: "2rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "0.5rem" }}>
          <div style={{ width: 26, height: 2, background: "#E8B14C" }} />
          <span style={{ color: "#E8B14C", fontSize: "0.72rem", fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase" }}>
            Film search
          </span>
        </div>
        <h1 style={{ margin: 0, fontSize: "2rem" }}>Search the catalog</h1>
      </div>

      <form onSubmit={handleSearch} style={{ display: "flex", gap: "0.75rem", marginBottom: "2rem" }}>
        <input
          value={term}
          onChange={e => setTerm(e.target.value)}
          placeholder="Search by title…"
          style={{
            flex: 1, background: "#16161C", border: "1px solid #24242c",
            borderRadius: 8, padding: "0.75rem 1rem", color: "#EDE7DA",
            fontSize: "1rem", outline: "none",
          }}
        />
        <button type="submit" style={{
          background: "#E8B14C", color: "#0E0E12", border: "none",
          borderRadius: 8, padding: "0.75rem 1.5rem", fontWeight: 600,
          cursor: "pointer", fontSize: "0.95rem",
        }}>
          Search
        </button>
      </form>

      {loading && <p style={{ color: "#8A8779" }}>Searching…</p>}

      {results.length > 0 && (
        <>
          <p style={{ color: "#8A8779", marginBottom: "1rem" }}>
            {results.length} results
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
                  transition: "border-color 0.2s",
                }}>
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
                      {film.release_date?.slice(0, 4)} · ★ {Number(film.vote_average).toFixed(1)}
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