import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  const [movie, genres, credits, related] = await Promise.all([
    queryDatabricks(`
      SELECT film_id, title, release_date
      FROM workspace.silver_cinema_atlas.tmdb_films WHERE film_id = ${id}
    `),
    queryDatabricks(`
      SELECT genre_id, genre_name
      FROM workspace.silver_cinema_atlas.tmdb_film_genres
      WHERE film_id = ${id}
    `),
    queryDatabricks(`
      SELECT credit_type, person_id, person_name AS name, character, job, cast_order
      FROM workspace.silver_cinema_atlas.tmdb_credits
      WHERE film_id = ${id}
    `),
    queryDatabricks(`
      SELECT m.film_id, m.title
      FROM workspace.silver_cinema_atlas.tmdb_films m
      WHERE m.film_id IN (
        SELECT fg.film_id
        FROM workspace.silver_cinema_atlas.tmdb_film_genres fg
        WHERE fg.genre_id IN (
          SELECT genre_id FROM workspace.silver_cinema_atlas.tmdb_film_genres WHERE film_id = ${id}
        )
        AND fg.film_id != ${id}
      )
      AND m.vote_count >= 500
      ORDER BY m.vote_count DESC
      LIMIT 15
    `),
  ])

  const cast = credits
    .filter(c => c.credit_type === 'cast')
    .sort((a, b) => a.cast_order - b.cast_order)
    .slice(0, 10)
    .map(({ person_id, name, character }) => ({ person_id, name, character }))

  const crewJobs = ['Director', 'Screenplay', 'Writer', 'Director of Photography', 'Original Music Composer']
  const crew = credits
    .filter(c => c.credit_type === 'crew' && crewJobs.includes(c.job))
    .map(({ person_id, name, job }) => ({ person_id, name, job }))

  return Response.json({ movie: movie[0], genres, cast, crew, related })
}