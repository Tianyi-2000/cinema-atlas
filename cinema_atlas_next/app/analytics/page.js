"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import {
  BarChart, Bar, LineChart, Line,
  XAxis, YAxis, Tooltip, ResponsiveContainer, Cell
} from "recharts"

const IMG    = "https://image.tmdb.org/t/p/w185"
const AMBER  = "#E8B14C"
const PANEL  = "#16161C"
const BORDER = "#24242c"
const MUTED  = "#8A8779"
const CREAM  = "#EDE7DA"
const BG     = "#0E0E12"

// ── Shared components ──────────────────────────────────────────────────────

function Eyebrow({ text }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "1rem" }}>
      <div style={{ width: 26, height: 2, background: AMBER }} />
      <span style={{ color: AMBER, fontSize: "0.72rem", fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase" }}>
        {text}
      </span>
    </div>
  )
}

function TabBar({ tabs, active, onChange }) {
  return (
    <div style={{ display: "flex", gap: "0.5rem", marginBottom: "1.5rem", flexWrap: "wrap" }}>
      {tabs.map(t => (
        <button key={t.key} onClick={() => onChange(t.key)} style={{
          background: active === t.key ? AMBER : PANEL,
          color: active === t.key ? "#0E0E12" : MUTED,
          border: `1px solid ${active === t.key ? AMBER : BORDER}`,
          borderRadius: 8, padding: "0.5rem 1.2rem",
          cursor: "pointer", fontWeight: 600, fontSize: "0.88rem",
          transition: "all 0.15s"
        }}>
          {t.label}
        </button>
      ))}
    </div>
  )
}

// ── Film grid ──────────────────────────────────────────────────────────────

function FilmCard({ f, ratingField = "combined_rating" }) {
  const router = useRouter()
  const rating = Number(f[ratingField])
  const revenue = Number(f.revenue)
  return (
    <div onClick={() => router.push(`/film/${f.film_id}`)} style={{ cursor: "pointer" }}>
      <div style={{ background: PANEL, borderRadius: 8, border: `1px solid ${BORDER}`, overflow: "hidden" }}>
        {f.poster_path ? (
          <img src={`${IMG}${f.poster_path}`} alt={f.title} style={{ width: "100%", display: "block" }} />
        ) : (
          <div style={{
            aspectRatio: "2/3", background: "#24242c",
            display: "flex", alignItems: "center", justifyContent: "center",
            color: MUTED, fontSize: "0.75rem"
          }}>No poster</div>
        )}
        <div style={{ padding: "0.5rem 0.6rem" }}>
          <div style={{ fontSize: "0.8rem", fontWeight: 600, color: CREAM }}>
            {f.title?.slice(0, 22)}
          </div>
          <div style={{ fontSize: "0.72rem", color: MUTED, marginTop: "0.2rem" }}>
            {!isNaN(rating) && rating > 0 ? `★ ${rating.toFixed(1)}` : ""}
            {revenue > 0 ? ` · $${(revenue / 1e6).toFixed(0)}M` : ""}
          </div>
        </div>
      </div>
    </div>
  )
}

function FilmGrid({ films, ratingField }) {
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))",
      gap: "0.75rem", marginBottom: "2.5rem"
    }}>
      {films.map(f => <FilmCard key={f.film_id} f={f} ratingField={ratingField} />)}
    </div>
  )
}

// ── People leaderboard ─────────────────────────────────────────────────────

function PeopleTable({ people, sortKey, onSort }) {
  const cols = [
    { key: "rank_film_count", label: "# Films" },
    { key: "rank_avg_rating", label: "Avg Rating" },
    { key: "rank_revenue",    label: "Total Revenue" },
  ]
  return (
    <div style={{ background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 10, overflow: "hidden", marginBottom: "2.5rem" }}>
      {/* sort buttons */}
      <div style={{ display: "flex", gap: "0.5rem", padding: "0.75rem 1rem", borderBottom: `1px solid ${BORDER}` }}>
        <span style={{ color: MUTED, fontSize: "0.78rem", alignSelf: "center", marginRight: "0.5rem" }}>Sort by</span>
        {cols.map(c => (
          <button key={c.key} onClick={() => onSort(c.key)} style={{
            background: sortKey === c.key ? AMBER : "transparent",
            color: sortKey === c.key ? "#0E0E12" : MUTED,
            border: `1px solid ${sortKey === c.key ? AMBER : BORDER}`,
            borderRadius: 6, padding: "0.3rem 0.8rem",
            cursor: "pointer", fontSize: "0.78rem", fontWeight: 600
          }}>{c.label}</button>
        ))}
      </div>
      {/* table */}
      <div style={{ overflowY: "auto", maxHeight: 420 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
          <thead>
            <tr style={{ background: BG }}>
              <th style={{ padding: "0.6rem 1rem", textAlign: "left", color: MUTED, fontWeight: 600 }}>Rank</th>
              <th style={{ padding: "0.6rem 1rem", textAlign: "left", color: MUTED, fontWeight: 600 }}>Name</th>
              <th style={{ padding: "0.6rem 1rem", textAlign: "right", color: MUTED, fontWeight: 600 }}>Films</th>
              <th style={{ padding: "0.6rem 1rem", textAlign: "right", color: MUTED, fontWeight: 600 }}>Avg Rating</th>
              <th style={{ padding: "0.6rem 1rem", textAlign: "right", color: MUTED, fontWeight: 600 }}>Revenue</th>
            </tr>
          </thead>
          <tbody>
            {[...people]
              .sort((a, b) => Number(a[sortKey]) - Number(b[sortKey]))
              .slice(0, 25)
              .map((p, i) => (
                <tr key={p.person_id} style={{ borderTop: `1px solid ${BORDER}` }}>
                  <td style={{ padding: "0.6rem 1rem", color: AMBER, fontWeight: 700 }}>{i + 1}</td>
                  <td style={{ padding: "0.6rem 1rem", color: CREAM, fontWeight: 600 }}>{p.name}</td>
                  <td style={{ padding: "0.6rem 1rem", textAlign: "right", color: MUTED }}>{p.film_count}</td>
                  <td style={{ padding: "0.6rem 1rem", textAlign: "right", color: MUTED }}>
                    {Number(p.avg_rating) > 0 ? `★ ${Number(p.avg_rating).toFixed(2)}` : "—"}
                  </td>
                  <td style={{ padding: "0.6rem 1rem", textAlign: "right", color: MUTED }}>
                    {Number(p.total_revenue) > 0 ? `$${(Number(p.total_revenue) / 1e9).toFixed(2)}B` : "—"}
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Chart tooltip ──────────────────────────────────────────────────────────

const tooltipStyle = {
  contentStyle: { background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 8, color: CREAM },
  labelStyle: { color: AMBER }
}

// ── Main page ──────────────────────────────────────────────────────────────

export default function Analytics() {
  const [d, setD]               = useState(null)
  const [filmTab, setFilmTab]   = useState("rated")
  const [peopleTab, setPeopleTab] = useState("directors")
  const [dirSort, setDirSort]   = useState("rank_film_count")
  const [actSort, setActSort]   = useState("rank_film_count")

  useEffect(() => {
    fetch("/api/analytics").then(r => r.json()).then(setD)
  }, [])

  if (!d) return <p style={{ color: MUTED, padding: "2rem" }}>Loading…</p>

  const filmTabs = [
    { key: "rated",   label: "Highest rated" },
    { key: "revenue", label: "Highest revenue" },
    { key: "popular", label: "Most popular" },
  ]

  const peopleTabs = [
    { key: "directors", label: "Directors" },
    { key: "actors",    label: "Actors" },
  ]

  return (
    <div>

      {/* ── Header ── */}
      <div style={{ marginBottom: "2rem" }}>
        <Eyebrow text="Analytics" />
        <h1 style={{ margin: 0, fontSize: "2rem" }}>Explore the catalog</h1>
      </div>

      {/* ── Film leaderboards ── */}
      <TabBar tabs={filmTabs} active={filmTab} onChange={setFilmTab} />

      {filmTab === "rated" && (
        <>
          <Eyebrow text="Highest rated films — combined IMDb + TMDB score (≥ 500 votes)" />
          <FilmGrid films={d.topRated} ratingField="combined_rating" />
        </>
      )}
      {filmTab === "revenue" && (
        <>
          <Eyebrow text="Highest grossing films" />
          <FilmGrid films={d.topRevenue} ratingField="combined_rating" />
        </>
      )}
      {filmTab === "popular" && (
        <>
          <Eyebrow text="Most popular films" />
          <FilmGrid films={d.topPopular} ratingField="combined_rating" />
        </>
      )}

      {/* ── Yearly trends ── */}
      <Eyebrow text="Films released per year" />
      <div style={{ background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 10, padding: "1rem", marginBottom: "2.5rem" }}>
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={d.yearlyTrends}>
            <XAxis dataKey="year" tick={{ fill: MUTED, fontSize: 11 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: MUTED, fontSize: 11 }} axisLine={false} tickLine={false} />
            <Tooltip {...tooltipStyle} />
            <Line type="monotone" dataKey="film_count" stroke={AMBER} strokeWidth={2} dot={false} name="Films" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <Eyebrow text="Average combined rating per year" />
      <div style={{ background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 10, padding: "1rem", marginBottom: "2.5rem" }}>
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={d.yearlyTrends}>
            <XAxis dataKey="year" tick={{ fill: MUTED, fontSize: 11 }} axisLine={false} tickLine={false} />
            <YAxis domain={[5, 9]} tick={{ fill: MUTED, fontSize: 11 }} axisLine={false} tickLine={false} />
            <Tooltip {...tooltipStyle} />
            <Line type="monotone" dataKey="avg_combined_rating" stroke={CREAM} strokeWidth={2} dot={false} name="Avg Rating" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* ── Genre charts ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2rem", marginBottom: "2.5rem" }}>
        <div>
          <Eyebrow text="Most common genres" />
          <div style={{ background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 10, padding: "1rem" }}>
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={d.genres} layout="vertical">
                <XAxis type="number" tick={{ fill: MUTED, fontSize: 11 }} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="genre" tick={{ fill: CREAM, fontSize: 12 }}
                  axisLine={false} tickLine={false} width={100} />
                <Tooltip {...tooltipStyle} />
                <Bar dataKey="films" radius={[0, 4, 4, 0]}>
                  {d.genres.map((_, i) => <Cell key={i} fill={AMBER} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div>
          <Eyebrow text="Best rated genres — combined score" />
          <div style={{ background: PANEL, border: `1px solid ${BORDER}`, borderRadius: 10, padding: "1rem" }}>
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={d.ratingByGenre} layout="vertical">
                <XAxis type="number" domain={[5, 8.5]} tick={{ fill: MUTED, fontSize: 11 }}
                  axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="genre" tick={{ fill: CREAM, fontSize: 12 }}
                  axisLine={false} tickLine={false} width={100} />
                <Tooltip {...tooltipStyle} />
                <Bar dataKey="avg_rating" radius={[0, 4, 4, 0]}>
                  {d.ratingByGenre.map((_, i) => <Cell key={i} fill={CREAM} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* ── People leaderboards ── */}
      <TabBar tabs={peopleTabs} active={peopleTab} onChange={setPeopleTab} />

      {peopleTab === "directors" && (
        <>
          <Eyebrow text="Top directors — sortable by films, rating, or revenue" />
          <PeopleTable people={d.topDirectors} sortKey={dirSort} onSort={setDirSort} />
        </>
      )}
      {peopleTab === "actors" && (
        <>
          <Eyebrow text="Top actors — sortable by films, rating, or revenue" />
          <PeopleTable people={d.topActors} sortKey={actSort} onSort={setActSort} />
        </>
      )}

    </div>
  )
}