export async function GET(request, { params }) {
  const { id } = await params
  const TMDB_KEY = process.env.TMDB_API_KEY

  try {
    const res = await fetch(
      `https://api.themoviedb.org/3/movie/${id}/videos?api_key=${TMDB_KEY}`,
      { cache: "no-store" }
    )
    const data = await res.json()
    const yt = (data.results || []).filter(v => v.site === "YouTube")
    const trailer = yt.find(v => v.type === "Trailer" && v.official)
      || yt.find(v => v.type === "Trailer")
      || yt[0]
    return Response.json({ key: trailer?.key || null })
  } catch {
    return Response.json({ key: null })
  }
}