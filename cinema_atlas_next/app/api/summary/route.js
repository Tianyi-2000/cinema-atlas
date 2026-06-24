import { queryDatabricks } from "@/lib/databricks"

export async function GET() {
  const rows = await queryDatabricks(`
    SELECT
      (SELECT COUNT(*) FROM milkmoo.silver.movies)                 AS films,
      (SELECT COUNT(*) FROM milkmoo.silver.people)                 AS people,
      (SELECT COUNT(*) FROM milkmoo.silver.film_reviews)           AS reviews,
      (SELECT COUNT(DISTINCT genre_id) FROM milkmoo.silver.genres) AS genres,
      (SELECT MIN(release_date) FROM milkmoo.silver.movies)        AS earliest,
      (SELECT MAX(release_date) FROM milkmoo.silver.movies)        AS latest
  `)
  return Response.json(rows[0])
}