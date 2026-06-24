"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import {
  BarChart, Bar, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Cell
} from "recharts"

const IMG = "https://image.tmdb.org/t/p/w185"
const AMBER = "#E8B14C"
const PANEL = "#16161C"
const BORDER = "#24242c"
const MUTED = "#8A8779"
const CREAM = "#EDE7DA"

function Eyebrow({ text }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "1rem" }}>
      <div style={{ width: 26, height: 2, background: AMBER }} />
      <span style={{
        color: AMBER, fontSize: "0.72rem",
        fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase"
      }}>{text}</span>
    </div>
  )
}

function FilmGrid({ films }) {
  const router = useRouter()
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))",
      gap: "0.75rem", marginBottom: "2.5rem"
    }}>
      {films.map(f => (
        <div key={f.film_id} onClick={() => router.push(`/film/${f.film_id}`)}
          style={{ cursor: "pointer" }}>
          <div style={{
            background: PANEL, borderRadius: 8,
            border: `1px solid ${BORDER}`, overflow: "hidden"
          }}>
            {f.poster_path ? (
              <img src={`${IMG}${f.poster_path}`} alt={f.title}
                style={{ width: "100%", display: "block" }} />
            ) : (
              <div style={{
                aspectRatio: "2/3", background: "#24242c",
                display: "flex", alignItems: "center",
                justifyContent: "center", color: MUTED, fontSize: "0.75rem"
              }}>No poster</div>
            )}
            <div style={{ padding: "0.5rem 0.6rem" }}>
              <div style={{ fontSize: "0.8rem", fontWeight: 600, color: CREAM }}>
                {f.title?.slice(0, 22)}
              </div>
              <div style={{ fontSize: "0.72rem", color: MUTED, marginTop: "0.2rem" }}>
                ★ {Number(f.vote_average).toFixed(1)}
                {Number(f.revenue) > 0 && ` · $${(Number(f.revenue) / 1e6).toFixed(0)}M`}
              </div>
            </div>
          </div>
        </div>
      ))}
    </div>
  )
}

export default function Analytics() {
  const [d, setD] = useState(null)
  const [tab, setTab] = useState("rated")

  useEffect(() => {
    fetch("/api/analytics").then(r => r.json()).then(setD)
  }, [])

  if (!d) return <p style={{ color: MUTED }}>Loading…</p>

  return (
    <div>
      <div style={{ marginBottom: "2rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "0.5rem" }}>
          <div style={{ width: 26, height: 2, background: AMBER }} />
          <span style={{ color: AMBER, fontSize: "0.72rem", fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase" }}>
            Analytics
          </span>
        </div>
        <h1 style={{ margin: 0, fontSize: "2rem" }}>Explore the catalog</h1>
      </div>

      {/* leaderboard tabs */}
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1.5rem" }}>
        {[
          { key: "rated", label: "Highest rated" },
          { key: "revenue", label: "Highest revenue" },
        ].map(t => (
          <button key={t.key} onClick={() => setTab(t.key)} style={{
            background: tab === t.key ? AMBER : PANEL,
            color: tab === t.key ? "#0E0E12" : MUTED,
            border: `1px solid ${BORDER}`, borderRadius: 8,
            padding: "0.5rem 1.2rem", cursor: "pointer",
            fontWeight: 600, fontSize: "0.88rem"
          }}>
            {t.label}
          </button>
        ))}
      </div>

      {tab === "rated" && (
        <>
          <Eyebrow text="Highest rated films (≥ 500 votes)" />
          <FilmGrid films={d.topRated} />
        </>
      )}

      {tab === "revenue" && (
        <>
          <Eyebrow text="Highest grossing films" />
          <FilmGrid films={d.topRevenue} />
        </>
      )}

      {/* genre charts */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2rem" }}>
        <div>
          <Eyebrow text="Most common genres" />
          <div style={{ background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 10, padding: "1rem" }}>
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={d.genres} layout="vertical">
                <XAxis type="number" tick={{ fill: MUTED, fontSize: 11 }} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="genre" tick={{ fill: CREAM, fontSize: 12 }}
                  axisLine={false} tickLine={false} width={100} />
                <Tooltip contentStyle={{ background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 8, color: CREAM }} />
                <Bar dataKey="films" radius={[0, 4, 4, 0]}>
                  {d.genres.map((_, i) => <Cell key={i} fill={AMBER} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div>
          <Eyebrow text="Best rated genres" />
          <div style={{ background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 10, padding: "1rem" }}>
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={d.ratingByGenre} layout="vertical">
                <XAxis type="number" domain={[5, 8.5]} tick={{ fill: MUTED, fontSize: 11 }}
                  axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="genre" tick={{ fill: CREAM, fontSize: 12 }}
                  axisLine={false} tickLine={false} width={100} />
                <Tooltip contentStyle={{ background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 8, color: CREAM }} />
                <Bar dataKey="avg_rating" radius={[0, 4, 4, 0]}>
                  {d.ratingByGenre.map((_, i) => <Cell key={i} fill={CREAM} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  )
}