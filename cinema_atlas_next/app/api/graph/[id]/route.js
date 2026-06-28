import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  const [movie, genres, cast, crew, related] = await Promise.all([
    queryDatabricks(`
      SELECT film_id, title, release_date
      FROM workspace.silver.movies WHERE film_id = ${id}
    `),
    queryDatabricks(`
      SELECT g.genre_id, g.genre_name
      FROM workspace.silver.film_genres fg
      JOIN workspace.silver.genres g ON fg.genre_id = g.genre_id
      WHERE fg.film_id = ${id}
    `),
    queryDatabricks(`
      SELECT p.person_id, p.name, c.character
      FROM workspace.silver.film_cast c
      JOIN workspace.silver.people p ON c.person_id = p.person_id
      WHERE c.film_id = ${id}
      ORDER BY c.cast_order LIMIT 10
    `),
    queryDatabricks(`
      SELECT p.person_id, p.name, c.job
      FROM workspace.silver.film_crew c
      JOIN workspace.silver.people p ON c.person_id = p.person_id
      WHERE c.film_id = ${id}
        AND c.job IN ('Director','Screenplay','Writer',
                      'Director of Photography','Original Music Composer')
    `),
    queryDatabricks(`
      SELECT m.film_id, m.title
      FROM workspace.silver.movies m
      WHERE m.film_id IN (
        SELECT fg.film_id
        FROM workspace.silver.film_genres fg
        WHERE fg.genre_id IN (
          SELECT genre_id FROM workspace.silver.film_genres WHERE film_id = ${id}
        )
        AND fg.film_id != ${id}
      )
      AND m.vote_count >= 500
      ORDER BY m.vote_count DESC
      LIMIT 15
    `),
  ])

  return Response.json({ movie: movie[0], genres, cast, crew, related })
}