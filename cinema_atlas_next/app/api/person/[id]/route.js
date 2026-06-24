import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  const films = await queryDatabricks(`
    SELECT DISTINCT m.film_id, m.title, m.release_date, c.job, c.character
    FROM (
      SELECT film_id, person_id, job, NULL AS character FROM milkmoo.silver.film_crew
      WHERE person_id = ${id}
        AND job IN ('Director','Screenplay','Writer',
                    'Director of Photography','Original Music Composer','Producer')
      UNION ALL
      SELECT film_id, person_id, NULL AS job, character FROM milkmoo.silver.film_cast
      WHERE person_id = ${id}
    ) c
    JOIN milkmoo.silver.movies m ON c.film_id = m.film_id
    WHERE m.vote_count >= 200
    ORDER BY m.release_date DESC
    LIMIT 10
  `)

  return Response.json(films)
}