import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  const films = await queryDatabricks(`
    SELECT m.film_id, m.title, m.release_date, c.job, c.character
    FROM workspace.silver_cinema_atlas.tmdb_credits c
    JOIN workspace.silver_cinema_atlas.tmdb_films m ON c.film_id = m.film_id
    WHERE c.person_id = ${id}
      AND (
        (c.credit_type = 'crew' AND c.job IN ('Director','Screenplay','Writer',
                    'Director of Photography','Original Music Composer','Producer'))
        OR c.credit_type = 'cast'
      )
      AND m.vote_count >= 200
    ORDER BY m.release_date DESC
    LIMIT 10
  `)

  return Response.json(films)
}