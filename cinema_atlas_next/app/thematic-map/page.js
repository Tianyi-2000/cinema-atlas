"use client"

export default function ThematicMap() {
  return (
    <div>
      <div style={{ marginBottom: "1rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "0.4rem" }}>
          <div style={{ width: 26, height: 2, background: "#E8B14C" }} />
          <span style={{ color: "#E8B14C", fontSize: "0.72rem", fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase" }}>
            Thematic Map
          </span>
        </div>
        <h1 style={{ margin: 0, fontSize: "1.8rem" }}>Cinema Landscape</h1>
        <p style={{ color: "#8A8779", margin: "0.4rem 0 0", fontSize: "0.88rem" }}>
          10,000+ films positioned by plot similarity. Films cluster by theme — hover to explore connections.
          Drag to pan, scroll to zoom, click to search.
        </p>
      </div>

      <div style={{ height: "calc(100vh - 180px)", borderRadius: 10, overflow: "hidden", border: "1px solid #24242c" }}>
        <iframe
          src="/cinema_atlas_thematic_map.html"
          style={{ width: "100%", height: "100%", border: "none", display: "block" }}
          title="Cinema Atlas Thematic Map"
        />
      </div>
    </div>
  )
}
