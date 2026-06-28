import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  const [movie, genres, cast, crew, reviews, history] = await Promise.all([
    queryDatabricks(`SELECT * FROM workspace.silver.movies WHERE film_id = ${id}`),
    queryDatabricks(`
      SELECT g.genre_name FROM workspace.silver.film_genres fg
      JOIN workspace.silver.genres g ON fg.genre_id = g.genre_id
      WHERE fg.film_id = ${id}
    `),
    queryDatabricks(`
      SELECT c.cast_order, c.character, p.person_id, p.name, p.profile_path
      FROM workspace.silver.film_cast c
      JOIN workspace.silver.people p ON c.person_id = p.person_id
      WHERE c.film_id = ${id}
      ORDER BY c.cast_order LIMIT 12
    `),
    queryDatabricks(`
      SELECT c.job, p.name
      FROM workspace.silver.film_crew c
      JOIN workspace.silver.people p ON c.person_id = p.person_id
      WHERE c.film_id = ${id}
        AND c.job IN ('Director','Screenplay','Writer',
                      'Director of Photography','Original Music Composer','Producer')
      ORDER BY CASE c.job WHEN 'Director' THEN 1 WHEN 'Screenplay' THEN 2 ELSE 3 END
    `),
    queryDatabricks(`
      SELECT author, author_rating, content, created_at
      FROM workspace.silver.film_reviews
      WHERE film_id = ${id}
      ORDER BY created_at DESC LIMIT 5
    `),
    queryDatabricks(`
      SELECT snapshot_ts, revenue, popularity, vote_count
      FROM workspace.silver.audience_trends
      WHERE film_id = ${id}
      ORDER BY snapshot_ts
    `),
  ])

  return Response.json({
    movie: movie[0],
    genres,
    cast,
    crew,
    reviews,
    history,
  })
}