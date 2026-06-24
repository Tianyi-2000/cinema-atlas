"use client"

import { useEffect, useState } from "react"
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts"

export default function Home() {
  const [summary, setSummary] = useState(null)
  const [filmsPerYear, setFilmsPerYear] = useState([])

  useEffect(() => {
    fetch("/api/summary").then(r => r.json()).then(setSummary)
    fetch("/api/films-per-year").then(r => r.json()).then(setFilmsPerYear)
  }, [])

  return (
    <div>
      {/* header */}
      <div style={{ marginBottom: "2rem" }}>
        <div style={{
          display: "flex", alignItems: "center", gap: "0.6rem",
          marginBottom: "0.5rem"
        }}>
          <div style={{ width: 26, height: 2, background: "#E8B14C" }} />
          <span style={{
            color: "#E8B14C", fontSize: "0.72rem",
            fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase"
          }}>
            Cinema Atlas
          </span>
        </div>
        <h1 style={{ margin: 0, fontSize: "2.8rem", fontWeight: 600 }}>
          A living map of cinema
        </h1>
        <p style={{ color: "#8A8779", marginTop: "0.5rem", maxWidth: "60ch" }}>
          Built on the TMDB, IMDB and Wikidata as sources — films, people, genres, releases,
          reviews, and a box-office time series.
        </p>
      </div>

      {/* metric cards */}
      {summary && (
        <div style={{
          display: "grid",
          gridTemplateColumns: "repeat(5, 1fr)",
          gap: "1rem",
          marginBottom: "2.5rem"
        }}>
          {[
            { value: Number(summary.films).toLocaleString(), label: "Films" },
            { value: Number(summary.people).toLocaleString(), label: "People" },
            { value: Number(summary.reviews).toLocaleString(), label: "Reviews" },
            { value: summary.genres, label: "Genres" },
            { value: `${summary.earliest?.slice(0,4)}–${summary.latest?.slice(0,4)}`, label: "Release span" },
          ].map(({ value, label }) => (
            <div key={label} style={{
              background: "#16161C",
              border: "1px solid #24242c",
              borderRadius: 10,
              padding: "1rem 1.2rem",
            }}>
              <div style={{
                fontFamily: "Georgia, serif",
                fontSize: "1.9rem",
                fontWeight: 600,
                color: "#EDE7DA",
                lineHeight: 1,
              }}>
                {value}
              </div>
              <div style={{
                fontSize: "0.72rem",
                letterSpacing: "0.1em",
                textTransform: "uppercase",
                color: "#8A8779",
                marginTop: "0.4rem",
              }}>
                {label}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* releases over time */}
      <div style={{ marginBottom: "0.75rem", display: "flex", alignItems: "center", gap: "0.6rem" }}>
        <div style={{ width: 26, height: 2, background: "#E8B14C" }} />
        <span style={{
          color: "#E8B14C", fontSize: "0.72rem",
          fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase"
        }}>
          Releases over time
        </span>
      </div>

      {filmsPerYear.length > 0 && (
        <div style={{
          background: "#16161C",
          border: "1px solid #24242c",
          borderRadius: 10,
          padding: "1.5rem",
          marginBottom: "2rem",
        }}>
          <ResponsiveContainer width="100%" height={260}>
            <AreaChart data={filmsPerYear}>
              <defs>
                <linearGradient id="amberGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#E8B14C" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#E8B14C" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="year"
                stroke="#8A8779"
                tick={{ fill: "#8A8779", fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                stroke="#8A8779"
                tick={{ fill: "#8A8779", fontSize: 12 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                contentStyle={{
                  background: "#16161C",
                  border: "1px solid #24242c",
                  borderRadius: 8,
                  color: "#EDE7DA",
                }}
              />
              <Area
                type="monotone"
                dataKey="films"
                stroke="#E8B14C"
                strokeWidth={2}
                fill="url(#amberGrad)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}

      {!summary && (
        <p style={{ color: "#8A8779" }}>Loading…</p>
      )}
    </div>
  )
}