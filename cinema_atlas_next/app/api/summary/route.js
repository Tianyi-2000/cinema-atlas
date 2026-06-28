import { queryDatabricks } from "@/lib/databricks"

export async function GET() {
  const rows = await queryDatabricks(`
    SELECT
      (SELECT COUNT(*) FROM workspace.silver.movies)                 AS films,
      (SELECT COUNT(*) FROM workspace.silver.people)                 AS people,
      (SELECT COUNT(*) FROM workspace.silver.film_reviews)           AS reviews,
      (SELECT COUNT(DISTINCT genre_id) FROM workspace.silver.genres) AS genres,
      (SELECT MIN(release_date) FROM workspace.silver.movies)        AS earliest,
      (SELECT MAX(release_date) FROM workspace.silver.movies)        AS latest
  `)
  return Response.json(rows[0])
}