import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  try {
    const results = await Promise.allSettled([
      queryDatabricks(`
        SELECT m.*, sf.tconst
        FROM workspace.silver_cinema_atlas.tmdb_films m
        LEFT JOIN workspace.silver.films sf ON m.film_id = sf.id
        WHERE m.film_id = ${id}
      `),
      queryDatabricks(`
        SELECT credit_type, cast_order, character, job, person_id, person_name AS name, profile_path
        FROM workspace.silver_cinema_atlas.tmdb_credits
        WHERE film_id = ${id}
      `),
      queryDatabricks(`
        SELECT author, author_rating, content, created_at
        FROM workspace.silver_cinema_atlas.tmdb_reviews
        WHERE film_id = ${id}
        ORDER BY created_at DESC LIMIT 5
      `),
      queryDatabricks(`
        SELECT snapshot_ts, revenue, popularity, vote_count
        FROM workspace.silver_cinema_atlas.tmdb_audience_trends
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

    const val = (i) => results[i].status === 'fulfilled' ? results[i].value : []

    const movie = val(0)[0] ?? null
    const genres = movie?.genres
      ? movie.genres.split('|').filter(Boolean).map(genre_name => ({ genre_name }))
      : []

    const credits = val(1)
    const cast = credits
      .filter(c => c.credit_type === 'cast')
      .sort((a, b) => a.cast_order - b.cast_order)
      .slice(0, 12)
    const crewOrder = ['Director', 'Screenplay', 'Writer', 'Director of Photography', 'Original Music Composer', 'Producer']
    const crew = credits
      .filter(c => c.credit_type === 'crew' && crewOrder.includes(c.job))
      .sort((a, b) => crewOrder.indexOf(a.job) - crewOrder.indexOf(b.job))

    return Response.json({
      movie,
      genres,
      cast,
      crew,
      reviews:     val(2),
      history:     val(3),
      imdbRatings: val(4),
      akas:        val(5),
    })
  } catch (err) {
    console.error('film API error:', err)
    return Response.json({ error: err.message }, { status: 500 })
  }
}