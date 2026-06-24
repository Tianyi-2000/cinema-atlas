import { queryDatabricks } from "@/lib/databricks"

export async function GET() {
  const [topRated, topRevenue, genres, ratingByGenre] = await Promise.all([
    queryDatabricks(`
      SELECT film_id, title, release_date, vote_average, vote_count,
             revenue, poster_path
      FROM milkmoo.silver.movies
      WHERE vote_count >= 500
      ORDER BY vote_average DESC LIMIT 24
    `),
    queryDatabricks(`
      SELECT film_id, title, release_date, vote_average, vote_count,
             revenue, poster_path
      FROM milkmoo.silver.movies
      WHERE revenue > 0 AND vote_count >= 200
      ORDER BY revenue DESC LIMIT 24
    `),
    queryDatabricks(`
      SELECT g.genre_name AS genre, COUNT(*) AS films
      FROM milkmoo.silver.film_genres fg
      JOIN milkmoo.silver.genres g ON fg.genre_id = g.genre_id
      GROUP BY g.genre_name ORDER BY films DESC
    `),
    queryDatabricks(`
      SELECT g.genre_name AS genre,
             ROUND(AVG(m.vote_average), 2) AS avg_rating,
             COUNT(*) AS films
      FROM milkmoo.silver.film_genres fg
      JOIN milkmoo.silver.genres g ON fg.genre_id = g.genre_id
      JOIN milkmoo.silver.movies m ON fg.film_id = m.film_id
      WHERE m.vote_count > 50
      GROUP BY g.genre_name
      HAVING COUNT(*) > 20
      ORDER BY avg_rating DESC
    `),
  ])

  return Response.json({ topRated, topRevenue, genres, ratingByGenre })
}