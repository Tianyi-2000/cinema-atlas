import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  const [movie, genres, cast, crew, reviews, history, imdbRatings, akas] = await Promise.all([
    queryDatabricks(`
      SELECT m.*, sf.tconst
      FROM milkmoo.silver.movies m
      LEFT JOIN workspace.silver.films sf ON m.film_id = sf.id
      WHERE m.film_id = ${id}
    `),
    queryDatabricks(`
      SELECT g.genre_name FROM milkmoo.silver.film_genres fg
      JOIN milkmoo.silver.genres g ON fg.genre_id = g.genre_id
      WHERE fg.film_id = ${id}
    `),
    queryDatabricks(`
      SELECT c.cast_order, c.character, p.person_id, p.name, p.profile_path
      FROM milkmoo.silver.film_cast c
      JOIN milkmoo.silver.people p ON c.person_id = p.person_id
      WHERE c.film_id = ${id}
      ORDER BY c.cast_order LIMIT 12
    `),
    queryDatabricks(`
      SELECT c.job, p.name
      FROM milkmoo.silver.film_crew c
      JOIN milkmoo.silver.people p ON c.person_id = p.person_id
      WHERE c.film_id = ${id}
        AND c.job IN ('Director','Screenplay','Writer',
                      'Director of Photography','Original Music Composer','Producer')
      ORDER BY CASE c.job WHEN 'Director' THEN 1 WHEN 'Screenplay' THEN 2 ELSE 3 END
    `),
    queryDatabricks(`
      SELECT author, author_rating, content, created_at
      FROM milkmoo.silver.film_reviews
      WHERE film_id = ${id}
      ORDER BY created_at DESC LIMIT 5
    `),
    queryDatabricks(`
      SELECT snapshot_ts, revenue, popularity, vote_count
      FROM milkmoo.silver.audience_trends
      WHERE film_id = ${id}
      ORDER BY snapshot_ts
    `),
    queryDatabricks(`
      SELECT ir.snapshot_date, ir.averageRating, ir.numVotes
      FROM workspace.bronze.imdb_ratings_validated ir
      JOIN workspace.silver.matched_tconsts mt ON ir.tconst = mt.tconst
      WHERE mt.film_id = ${id}
      ORDER BY ir.snapshot_date
    `),
    queryDatabricks(`
      SELECT ia.title, ia.region, ia.language, ia.isOriginalLanguage
      FROM workspace.bronze.imdb_akas_validated ia
      JOIN workspace.silver.films sf ON ia.titleId = sf.tconst
      WHERE sf.id = ${id}
        AND ia.region IS NOT NULL
      ORDER BY CASE WHEN ia.isOriginalLanguage = 'true' THEN 0 ELSE 1 END, ia.ordering
      LIMIT 8
    `),
  ])

  return Response.json({
    movie: movie[0],
    genres,
    cast,
    crew,
    reviews,
    history,
    imdbRatings,
    akas,
  })
}
