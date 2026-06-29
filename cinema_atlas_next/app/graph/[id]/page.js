"use client"

import { useEffect, useRef, useState } from "react"
import { useParams, useRouter } from "next/navigation"

export default function GraphPage() {
  const { id } = useParams()
  const router = useRouter()
  const containerRef = useRef(null)
  const cyRef = useRef(null)
  const [data, setData] = useState(null)
  const [info, setInfo] = useState(null)
  const [expanded, setExpanded] = useState(new Set())
  const [expanding, setExpanding] = useState(false)

  useEffect(() => {
    if (!id) return
    Promise.all([
      fetch(`/api/graph/${id}`).then(r => r.json()),
      fetch(`/api/crew-connections/${id}`).then(r => r.json()).catch(() => []),
      fetch(`/api/based-on/${id}`).then(r => r.json()).catch(() => []),
    ]).then(([graph, crewConns, basedOn]) => {
      setData({ ...graph, crewConns, basedOn })
    })
  }, [id])

  useEffect(() => {
    if (!data || !containerRef.current) return

    import("cytoscape").then(mod => {
      const cytoscape = mod.default
      if (cyRef.current) cyRef.current.destroy()

      const { movie, genres, cast, crew, crewConns = [], basedOn = [] } = data
      const yr = movie?.release_date?.slice(0, 4)
      const elements = []
      const seen = new Set()

      function addNode(node) {
        if (!seen.has(node.data.id)) {
          seen.add(node.data.id)
          elements.push(node)
        }
      }
      function addEdge(edge) {
        elements.push(edge)
      }

      // Center film
      addNode({ data: { id: `film_${id}`, label: `${movie?.title} (${yr})`, type: "center" } })

      // ── Crew-bridge connections (film → crew person → other films) ──
      crewConns.forEach(person => {
        const pid = `person_${person.person_id}`
        addNode({
          data: {
            id: pid,
            label: person.name,
            type: "crew_bridge",
            role: person.role,
            person_id: person.person_id,
          }
        })
        addEdge({ data: { id: `e_center_${pid}`, source: `film_${id}`, target: pid, label: person.role } })

        person.connectedFilms.forEach(f => {
          const fid = `crew_film_${f.film_id}`
          addNode({
            data: {
              id: fid,
              label: `${f.title} (${f.year ?? ""})`,
              type: "crew_film",
              film_id: f.film_id,
            }
          })
          addEdge({ data: { id: `e_${pid}_${fid}`, source: pid, target: fid } })
        })
      })

      // ── Source material connections (film → source → co-adapted films) ──
      basedOn.forEach(source => {
        const sid = `source_${source.source_wikidata_id}`
        addNode({
          data: {
            id: sid,
            label: source.source_label,
            type: "source",
          }
        })
        addEdge({ data: { id: `e_center_${sid}`, source: `film_${id}`, target: sid } })

        source.coAdaptedFilms.forEach(f => {
          const fid = `source_film_${f.film_id}`
          addNode({
            data: {
              id: fid,
              label: `${f.title} (${f.year ?? ""})`,
              type: "source_film",
              film_id: f.film_id,
            }
          })
          addEdge({ data: { id: `e_${sid}_${fid}`, source: sid, target: fid } })
        })
      })

      // ── Cast & crew person nodes (for expandable filmographies) ──
      // Only add genres and related if we have no crew connections (graceful fallback)
      if (crewConns.length === 0) {
        genres.forEach(g => {
          addNode({ data: { id: `genre_${g.genre_id}`, label: g.genre_name, type: "genre" } })
          addEdge({ data: { id: `e_f_g_${g.genre_id}`, source: `film_${id}`, target: `genre_${g.genre_id}` } })
        })
        const related = data.related || []
        related.forEach(r => {
          addNode({ data: { id: `film_${r.film_id}`, label: r.title, type: "related", film_id: r.film_id } })
          const firstGenre = genres[0]
          if (firstGenre) {
            addEdge({ data: { id: `e_g_r_${r.film_id}`, source: `genre_${firstGenre.genre_id}`, target: `film_${r.film_id}` } })
          }
        })
      }

      cast.forEach(c => {
        addNode({
          data: {
            id: `person_${c.person_id}`,
            label: c.name,
            type: "person",
            role: c.character,
            person_id: c.person_id,
          }
        })
        addEdge({ data: { id: `e_f_p_${c.person_id}`, source: `film_${id}`, target: `person_${c.person_id}` } })
      })

      crew.forEach(c => {
        const pid = `person_${c.person_id}`
        addNode({
          data: {
            id: pid,
            label: c.name,
            type: "person",
            role: c.job,
            person_id: c.person_id,
          }
        })
        addEdge({ data: { id: `e_f_c_${c.person_id}_${c.job}`, source: `film_${id}`, target: pid } })
      })

      const cy = cytoscape({
        container: containerRef.current,
        elements,
        style: [
          {
            selector: "node",
            style: {
              label: "data(label)",
              "text-valign": "bottom",
              "text-halign": "center",
              "font-size": 10,
              color: "#EDE7DA",
              "text-outline-color": "#0E0E12",
              "text-outline-width": 2,
              "text-wrap": "wrap",
              "text-max-width": 110,
            }
          },
          {
            selector: "node[type='center']",
            style: {
              "background-color": "#E8B14C",
              width: 65, height: 65,
              "font-size": 13, "font-weight": "bold",
            }
          },
          {
            selector: "node[type='crew_bridge']",
            style: { "background-color": "#4a7c9e", width: 44, height: 44 }
          },
          {
            selector: "node[type='crew_film']",
            style: { "background-color": "#2a5472", width: 26, height: 26 }
          },
          {
            selector: "node[type='source']",
            style: { "background-color": "#7c6fcd", width: 36, height: 36 }
          },
          {
            selector: "node[type='source_film']",
            style: { "background-color": "#4a3d80", width: 22, height: 22 }
          },
          {
            selector: "node[type='genre']",
            style: { "background-color": "#5a9e6f", width: 40, height: 40 }
          },
          {
            selector: "node[type='person']",
            style: { "background-color": "#6b4f2e", width: 38, height: 38 }
          },
          {
            selector: "node[type='person_expanded']",
            style: { "background-color": "#4a3520", width: 38, height: 38 }
          },
          {
            selector: "node[type='related']",
            style: { "background-color": "#8a6e2f", width: 26, height: 26 }
          },
          {
            selector: "node[type='person_film']",
            style: { "background-color": "#5a4020", width: 22, height: 22 }
          },
          {
            selector: "edge",
            style: { width: 1, "line-color": "#24242c", opacity: 0.65 }
          },
          {
            selector: "node:selected",
            style: { "border-width": 3, "border-color": "#E8B14C" }
          },
        ],
        layout: {
          name: "cose",
          idealEdgeLength: 220,
          nodeOverlap: 30,
          nodeRepulsion: () => 9000,
          edgeElasticity: () => 100,
          nestingFactor: 5,
          gravity: 0.25,
          numIter: 300,
          animate: false,
          randomize: true,
          padding: 60,
        }
      })

      cy.on("tap", "node[type='crew_film'], node[type='source_film'], node[type='related']", evt => {
        const fid = evt.target.data("film_id")
        setInfo({ label: evt.target.data("label"), type: evt.target.data("type"), nodeId: evt.target.id() })
        if (fid) router.push(`/graph/${fid}`)
      })

      cy.on("tap", "node[type='crew_bridge']", evt => {
        const node = evt.target
        setInfo({ label: node.data("label"), type: "crew_bridge", role: node.data("role"), nodeId: node.id() })
      })

      cy.on("tap", "node[type='source']", evt => {
        const d = evt.target.data()
        setInfo({ label: d.label, type: "source", nodeId: evt.target.id() })
      })

      cy.on("tap", "node[type='person'], node[type='person_expanded']", evt => {
        const node = evt.target
        const pid = node.data("person_id")
        const name = node.data("label")
        const role = node.data("role")
        setInfo({ label: name, type: node.data("type"), role, nodeId: node.id() })
        if (!pid || expanded.has(pid)) return

        setExpanding(true)
        fetch(`/api/person/${pid}`)
          .then(r => r.json())
          .then(films => {
            node.data("type", "person_expanded")
            films.forEach(f => {
              const fNodeId = `pfilm_${f.film_id}`
              if (!cy.getElementById(fNodeId).length) {
                cy.add({
                  data: {
                    id: fNodeId,
                    label: `${f.title} (${f.release_date?.slice(0, 4)})`,
                    type: "person_film",
                    film_id: f.film_id,
                  }
                })
              }
              const eId = `e_p_pf_${pid}_${f.film_id}`
              if (!cy.getElementById(eId).length) {
                cy.add({ data: { id: eId, source: `person_${pid}`, target: fNodeId } })
              }
            })
            cy.layout({
              name: "cose",
              idealEdgeLength: 220,
              nodeOverlap: 30,
              nodeRepulsion: () => 9000,
              edgeElasticity: () => 100,
              nestingFactor: 5,
              gravity: 0.25,
              numIter: 300,
              animate: false,
              randomize: false,
              padding: 60,
            }).run()
            setExpanded(prev => new Set([...prev, pid]))
            setExpanding(false)
          })
          .catch(() => setExpanding(false))
      })

      cy.on("tap", "node[type='person_film']", evt => {
        const d = evt.target.data()
        setInfo({ label: d.label, type: "person_film", nodeId: evt.target.id() })
        if (d.film_id) router.push(`/graph/${d.film_id}`)
      })

      cy.on("tap", "node[type='center'], node[type='genre']", evt => {
        const d = evt.target.data()
        setInfo({ label: d.label, type: d.type, role: d.role, nodeId: evt.target.id() })
      })

      cyRef.current = cy
    })

    return () => {
      if (cyRef.current) { cyRef.current.destroy(); cyRef.current = null }
    }
  }, [data, id, router])

  function removeNode() {
    const cy = cyRef.current
    if (!cy || !info?.nodeId) return
    const node = cy.getElementById(info.nodeId)
    if (node) {
      node.connectedEdges().remove()
      node.remove()
    }
    setInfo(null)
  }

  return (
    <div>
      <div style={{ marginBottom: "1.5rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "0.4rem" }}>
          <div style={{ width: 26, height: 2, background: "#E8B14C" }} />
          <span style={{ color: "#E8B14C", fontSize: "0.72rem", fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase" }}>
            Connections
          </span>
        </div>
        {data?.movie && (
          <h1 style={{ margin: 0, fontSize: "1.8rem" }}>
            {data.movie.title}
            <span style={{ color: "#8A8779", fontWeight: 400, fontSize: "1.1rem" }}>
              {" "}({data.movie.release_date?.slice(0, 4)})
            </span>
          </h1>
        )}
        <p style={{ color: "#8A8779", margin: "0.4rem 0 0", fontSize: "0.88rem" }}>
          <span style={{ color: "#4a7c9e" }}>Crew nodes</span> connect to other films they worked on.
          Click a <span style={{ color: "#7c6fcd" }}>source node</span> to see co-adaptations.
          Click <span style={{ color: "#6b4f2e" }}>cast/crew</span> to expand their full filmography.
        </p>
      </div>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "1rem", flexWrap: "wrap", gap: "1rem" }}>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "1.2rem", fontSize: "0.82rem" }}>
          {[
            { color: "#E8B14C", label: "Selected film" },
            { color: "#4a7c9e", label: "Key crew (Director/DoP/Composer/Editor)" },
            { color: "#2a5472", label: "Films via shared crew" },
            { color: "#7c6fcd", label: "Source material" },
            { color: "#4a3d80", label: "Co-adapted films" },
            { color: "#6b4f2e", label: "Cast & crew (click to expand)" },
          ].map(({ color, label }) => (
            <div key={label} style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
              <div style={{ width: 10, height: 10, borderRadius: "50%", background: color }} />
              <span style={{ color: "#8A8779" }}>{label}</span>
            </div>
          ))}
        </div>

        <button
          onClick={() => router.push("/graph")}
          style={{
            background: "#16161C", color: "#E8B14C",
            border: "1px solid #E8B14C", borderRadius: 8,
            padding: "0.5rem 1.2rem", cursor: "pointer",
            fontWeight: 600, fontSize: "0.85rem", whiteSpace: "nowrap",
          }}
        >
          ← New search
        </button>
      </div>

      {info && (
        <div style={{
          background: "#16161C", border: "1px solid #24242c",
          borderRadius: 8, padding: "0.6rem 1rem", marginBottom: "1rem",
          fontSize: "0.88rem", display: "inline-flex", gap: "1rem",
          alignItems: "center", flexWrap: "wrap",
        }}>
          <span style={{ color: "#E8B14C", fontWeight: 600 }}>{info.label}</span>
          {info.role && <span style={{ color: "#8A8779" }}>{info.role}</span>}
          <span style={{ color: "#8A8779", fontSize: "0.75rem", textTransform: "uppercase" }}>
            {info.type?.replace(/_/g, " ")}
          </span>
          {expanding && <span style={{ color: "#8A8779", fontSize: "0.75rem" }}>Loading…</span>}
          {info.nodeId && info.type !== "center" && (
            <button
              onClick={removeNode}
              style={{
                background: "#2a1a1a", color: "#e05c5c",
                border: "1px solid #5c2a2a", borderRadius: 6,
                padding: "0.2rem 0.7rem", cursor: "pointer",
                fontSize: "0.78rem", fontWeight: 600,
              }}
            >
              Remove node
            </button>
          )}
          <button
            onClick={() => setInfo(null)}
            style={{ background: "none", border: "none", color: "#8A8779", cursor: "pointer", fontSize: "0.85rem" }}
          >
            ✕
          </button>
        </div>
      )}

      {!data && <p style={{ color: "#8A8779" }}>Loading graph…</p>}

      <div
        ref={containerRef}
        style={{
          width: "100%", height: "700px",
          background: "#16161C",
          border: "1px solid #24242c",
          borderRadius: 10,
        }}
      />
    </div>
  )
}
