import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  const rows = await queryDatabricks(`
    WITH center_crew AS (
      SELECT person_id, person_name AS name, job, profile_path
      FROM workspace.silver_cinema_atlas.tmdb_credits
      WHERE film_id = ${id}
        AND credit_type = 'crew'
        AND job IN (
          'Director', 'Director of Photography',
          'Original Music Composer', 'Editor'
        )
    )
    SELECT
      cc.person_id,
      cc.name   AS person_name,
      cc.job    AS role,
      cc.profile_path,
      m.film_id AS connected_film_id,
      m.title   AS connected_film_title,
      m.release_date,
      m.vote_average
    FROM center_crew cc
    JOIN workspace.silver_cinema_atlas.tmdb_credits c2
      ON cc.person_id = c2.person_id AND c2.credit_type = 'crew'
    JOIN workspace.silver_cinema_atlas.tmdb_films m ON c2.film_id = m.film_id
    JOIN workspace.silver.matched_tconsts mt ON m.imdb_id = mt.tconst
    WHERE c2.film_id != ${id}
      AND m.vote_count >= 200
    ORDER BY cc.person_id, CAST(m.vote_average AS DOUBLE) DESC
    LIMIT 150
  `)

  // Group flat rows by person_id
  const byPerson = {}
  for (const row of rows) {
    const pid = row.person_id
    if (!byPerson[pid]) {
      byPerson[pid] = {
        person_id: pid,
        name: row.person_name,
        role: row.role,
        profile_path: row.profile_path,
        connectedFilms: [],
      }
    }
    // Keep top 5 films per person
    if (byPerson[pid].connectedFilms.length < 5) {
      byPerson[pid].connectedFilms.push({
        film_id: row.connected_film_id,
        title: row.connected_film_title,
        year: row.release_date?.slice(0, 4),
        vote_average: row.vote_average,
      })
    }
  }

  return Response.json(Object.values(byPerson))
}