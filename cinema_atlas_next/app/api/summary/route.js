import { queryDatabricks } from "@/lib/databricks"

export async function GET() {
  const rows = await queryDatabricks(`
    SELECT
      (SELECT COUNT(*) FROM workspace.silver_cinema_atlas.tmdb_films)                         AS films,
      (SELECT COUNT(*) FROM workspace.silver_cinema_atlas.tmdb_people)                         AS people,
      (SELECT COUNT(*) FROM workspace.silver_cinema_atlas.tmdb_reviews)                        AS reviews,
      (SELECT COUNT(DISTINCT genre_id) FROM workspace.silver_cinema_atlas.tmdb_film_genres)    AS genres,
      (SELECT MIN(release_date) FROM workspace.silver_cinema_atlas.tmdb_films)                 AS earliest,
      (SELECT MAX(release_date) FROM workspace.silver_cinema_atlas.tmdb_films)                 AS latest
  `)
  return Response.json(rows[0])
}