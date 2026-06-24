import { queryDatabricks } from "@/lib/databricks"

export async function GET(request) {
  const { searchParams } = new URL(request.url)
  const term = searchParams.get("q") || ""

  const rows = await queryDatabricks(`
    SELECT film_id, title, release_date, vote_average, vote_count, poster_path
    FROM milkmoo.silver.movies
    WHERE LOWER(title) LIKE LOWER('%${term}%')
    ORDER BY vote_count DESC
    LIMIT 24
  `)
  return Response.json(rows)
}