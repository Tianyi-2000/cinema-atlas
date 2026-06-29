import "./globals.css"

export const metadata = {
  title: "Cinema Atlas",
  description: "A living map of cinema",
}

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body style={{ margin: 0, background: "#0E0E12" }}>
        <nav style={{
          background: "#16161C",
          borderBottom: "1px solid #24242c",
          padding: "0 2rem",
          height: "56px",
          display: "flex",
          alignItems: "center",
          gap: "2rem",
        }}>
          <span style={{
            color: "#E8B14C",
            fontFamily: "Georgia, serif",
            fontWeight: 600,
            fontSize: "1.1rem",
            letterSpacing: "-0.01em",
          }}>
            Cinema Atlas
          </span>
          <a href="/" style={{ color: "#8A8779", textDecoration: "none", fontSize: "0.9rem" }}>Home</a>
          <a href="/analytics" style={{ color: "#8A8779", textDecoration: "none", fontSize: "0.9rem" }}>Analytics</a>
          <a href="/search" style={{ color: "#8A8779", textDecoration: "none", fontSize: "0.9rem" }}>Films</a>
          <a href="/thematic-map" style={{ color: "#8A8779", textDecoration: "none", fontSize: "0.9rem" }}>Thematic Map</a>
          <a href="/director-map" style={{ color: "#8A8779", textDecoration: "none", fontSize: "0.9rem" }}>Director Map</a>
        </nav>
        <main style={{ padding: "2rem" }}>
          {children}
        </main>
      </body>
    </html>
  )
}