"use client"

import { useEffect, useState } from "react"
import { useParams } from "next/navigation"
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts"

const IMG_W500 = "https://image.tmdb.org/t/p/w500"
const IMG_W185 = "https://image.tmdb.org/t/p/w185"

export default function FilmProfile() {
  const { id } = useParams()
  const [data, setData] = useState(null)
  const [trailer, setTrailer] = useState(null)
  const [metric, setMetric] = useState("revenue")

  useEffect(() => {
    if (!id) return
    fetch(`/api/film/${id}`).then(r => r.json()).then(setData)
    fetch(`/api/trailer/${id}`).then(r => r.json()).then(d => setTrailer(d.key))
  }, [id])

  if (!data) return <p style={{ color: "#8A8779" }}>Loading…</p>

  const { movie, genres, cast, crew, reviews, history } = data
  const yr = movie?.release_date?.slice(0, 4)
  const rev = Number(movie?.revenue || 0)

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto" }}>

      {/* top section: poster + info */}
      <div style={{ display: "flex", gap: "2.5rem", marginBottom: "2.5rem" }}>

        {/* poster */}
        <div style={{ flexShrink: 0, width: 260 }}>
          {movie?.poster_path ? (
            <img
              src={`${IMG_W500}${movie.poster_path}`}
              alt={movie.title}
              style={{ width: "100%", borderRadius: 10 }}
            />
          ) : (
            <div style={{
              width: 260, aspectRatio: "2/3", background: "#16161C",
              borderRadius: 10, display: "flex", alignItems: "center",
              justifyContent: "center", color: "#8A8779"
            }}>No poster</div>
          )}
        </div>

        {/* info */}
        <div style={{ flex: 1 }}>
          <h1 style={{ margin: "0 0 0.2rem", fontSize: "2.2rem" }}>
            {movie?.title}
            <span style={{ color: "#8A8779", fontWeight: 400, fontSize: "1.4rem" }}> ({yr})</span>
          </h1>

          {movie?.tagline && (
            <p style={{ color: "#E8B14C", fontStyle: "italic", margin: "0 0 1rem" }}>
              "{movie.tagline}"
            </p>
          )}

          {/* genre chips */}
          <div style={{ display: "flex", flexWrap: "wrap", gap: "0.4rem", marginBottom: "1.2rem" }}>
            {genres.map(g => (
              <span key={g.genre_name} style={{
                background: "#23231b", color: "#E8B14C",
                border: "1px solid #3a3526", borderRadius: 999,
                padding: "0.2rem 0.8rem", fontSize: "0.78rem"
              }}>
                {g.genre_name}
              </span>
            ))}
          </div>

          {/* stats */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "1rem", marginBottom: "1.2rem" }}>
            {[
              { v: `${Number(movie?.vote_average || 0).toFixed(1)}`, l: "Rating" },
              { v: Number(movie?.vote_count || 0).toLocaleString(), l: "Votes" },
              { v: rev > 0 ? `$${(rev / 1e6).toFixed(0)}M` : "—", l: "Revenue" },
              { v: `${movie?.runtime || "—"}m`, l: "Runtime" },
            ].map(({ v, l }) => (
              <div key={l} style={{
                background: "#16161C", border: "1px solid #24242c",
                borderRadius: 8, padding: "0.8rem 1rem"
              }}>
                <div style={{ fontFamily: "Georgia, serif", fontSize: "1.5rem", color: "#EDE7DA" }}>{v}</div>
                <div style={{ fontSize: "0.7rem", letterSpacing: "0.1em", textTransform: "uppercase", color: "#8A8779" }}>{l}</div>
              </div>
            ))}
          </div>

          {/* overview */}
          {movie?.overview && (
            <p style={{ color: "#C9C3B6", lineHeight: 1.7, margin: "0 0 1rem" }}>
              {movie.overview}
            </p>
          )}

          {/* key crew */}
          {crew.length > 0 && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem 1.5rem", fontSize: "0.88rem" }}>
              {crew.map((c, i) => (
                <span key={i}>
                  <b style={{ color: "#EDE7DA" }}>{c.name}</b>
                  <span style={{ color: "#8A8779" }}> {c.job}</span>
                </span>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* trailer */}
      {trailer && (
        <div style={{ marginBottom: "2.5rem" }}>
          <Eyebrow text="Trailer" />
          <div style={{ borderRadius: 10, overflow: "hidden", marginTop: "0.75rem" }}>
            <iframe
              width="100%"
              height="480"
              src={`https://www.youtube.com/embed/${trailer}`}
              allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
              allowFullScreen
              style={{ display: "block", border: "none" }}
            />
          </div>
        </div>
      )}

      {/* cast */}
      {cast.length > 0 && (
        <div style={{ marginBottom: "2.5rem" }}>
          <Eyebrow text="Top billed cast" />
          <div style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(120px, 1fr))",
            gap: "1rem", marginTop: "0.75rem"
          }}>
            {cast.map(c => (
              <div key={c.person_id} style={{
                background: "#16161C", borderRadius: 8,
                border: "1px solid #24242c", overflow: "hidden"
              }}>
                {c.profile_path ? (
                  <img src={`${IMG_W185}${c.profile_path}`} alt={c.name}
                    style={{ width: "100%", display: "block" }} />
                ) : (
                  <div style={{
                    aspectRatio: "2/3", background: "#24242c",
                    display: "flex", alignItems: "center",
                    justifyContent: "center", color: "#8A8779", fontSize: "0.75rem"
                  }}>No photo</div>
                )}
                <div style={{ padding: "0.5rem 0.6rem" }}>
                  <div style={{ fontSize: "0.82rem", fontWeight: 600 }}>{c.name}</div>
                  {c.character && (
                    <div style={{ fontSize: "0.75rem", color: "#8A8779" }}>{c.character}</div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* box office time series */}
      {history.length > 1 && (
        <div style={{ marginBottom: "2.5rem" }}>
          <Eyebrow text="Metrics over time" />
          <div style={{ display: "flex", gap: "0.5rem", margin: "0.75rem 0" }}>
            {["revenue", "popularity", "vote_count"].map(m => (
              <button key={m} onClick={() => setMetric(m)} style={{
                background: metric === m ? "#E8B14C" : "#16161C",
                color: metric === m ? "#0E0E12" : "#8A8779",
                border: "1px solid #24242c", borderRadius: 6,
                padding: "0.3rem 0.8rem", cursor: "pointer", fontSize: "0.82rem"
              }}>
                {m.replace("_", " ")}
              </button>
            ))}
          </div>
          <div style={{
            background: "#16161C", border: "1px solid #24242c",
            borderRadius: 10, padding: "1rem"
          }}>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={history}>
                <XAxis dataKey="snapshot_ts" tick={{ fill: "#8A8779", fontSize: 11 }}
                  tickFormatter={v => v?.slice(0, 10)} axisLine={false} tickLine={false} />
                <YAxis tick={{ fill: "#8A8779", fontSize: 11 }} axisLine={false} tickLine={false} />
                <Tooltip contentStyle={{
                  background: "#16161C", border: "1px solid #24242c",
                  borderRadius: 8, color: "#EDE7DA"
                }} />
                <Line type="monotone" dataKey={metric}
                  stroke="#E8B14C" strokeWidth={2} dot={{ fill: "#E8B14C" }} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* reviews */}
      {reviews.length > 0 && (
        <div style={{ marginBottom: "2.5rem" }}>
          <Eyebrow text="Reviews" />
          <div style={{ marginTop: "0.75rem" }}>
            {reviews.map((r, i) => (
              <div key={i} style={{
                background: "#16161C", borderLeft: "3px solid #E8B14C",
                borderRadius: "0 8px 8px 0", padding: "0.8rem 1rem",
                marginBottom: "0.75rem"
              }}>
                <div style={{ color: "#E8B14C", fontWeight: 600, fontSize: "0.85rem" }}>
                  {r.author}{r.author_rating ? ` · ★ ${Number(r.author_rating).toFixed(0)}` : ""}
                </div>
                <div style={{ color: "#8A8779", fontSize: "0.88rem", marginTop: "0.3rem" }}>
                  {(r.content || "").slice(0, 600)}…
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function Eyebrow({ text }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.6rem" }}>
      <div style={{ width: 26, height: 2, background: "#E8B14C" }} />
      <span style={{
        color: "#E8B14C", fontSize: "0.72rem",
        fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase"
      }}>
        {text}
      </span>
    </div>
  )
}