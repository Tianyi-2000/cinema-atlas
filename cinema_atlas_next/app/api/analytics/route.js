import { queryDatabricks } from "@/lib/databricks"

export async function GET() {
  const [
    topRated,
    topRevenue,
    topPopular,
    genres,
    ratingByGenre,
    yearlyTrends,
    topDirectors,
    topActors,
  ] = await Promise.all([

    // ── Film leaderboards (Gold + poster_path from Silver) ──────────────────
    queryDatabricks(`
      SELECT g.film_id, g.title, g.year, g.combined_rating,
             g.imdb_rating, g.tmdb_rating, g.imdb_votes, g.tmdb_votes,
             g.revenue, g.rank_combined_rating,
             m.poster_path
      FROM workspace.gold.gold_film_leaderboard g
      LEFT JOIN workspace.silver.movies m ON g.film_id = m.film_id
      WHERE g.imdb_votes >= 500
      ORDER BY g.rank_combined_rating
      LIMIT 24
    `),

    queryDatabricks(`
      SELECT g.film_id, g.title, g.year, g.combined_rating,
             g.imdb_rating, g.tmdb_rating, g.revenue, g.profit,
             g.rank_revenue,
             m.poster_path
      FROM workspace.gold.gold_film_leaderboard g
      LEFT JOIN workspace.silver.movies m ON g.film_id = m.film_id
      WHERE g.revenue > 0
      ORDER BY g.rank_revenue
      LIMIT 24
    `),

    queryDatabricks(`
      SELECT g.film_id, g.title, g.year, g.combined_rating,
             g.imdb_rating, g.tmdb_rating, g.popularity,
             g.rank_popularity,
             m.poster_path
      FROM workspace.gold.gold_film_leaderboard g
      LEFT JOIN workspace.silver.movies m ON g.film_id = m.film_id
      ORDER BY g.rank_popularity
      LIMIT 24
    `),

    // ── Genre aggregations (Gold — all-time totals) ─────────────────────────
    queryDatabricks(`
      SELECT genre,
             SUM(film_count)                        AS films,
             ROUND(AVG(avg_combined_rating), 2)     AS avg_rating,
             SUM(total_revenue)                     AS total_revenue
      FROM workspace.gold.gold_genre_trends
      GROUP BY genre
      ORDER BY films DESC
    `),

    queryDatabricks(`
      SELECT genre,
             ROUND(AVG(avg_combined_rating), 2)     AS avg_rating,
             SUM(film_count)                        AS films
      FROM workspace.gold.gold_genre_trends
      GROUP BY genre
      HAVING SUM(film_count) > 20
      ORDER BY avg_rating DESC
    `),

    // ── Yearly trends (Gold) ────────────────────────────────────────────────
    queryDatabricks(`
      SELECT year, film_count, avg_combined_rating,
             avg_imdb_rating, avg_tmdb_rating,
             total_revenue, avg_revenue, avg_popularity
      FROM workspace.gold.gold_yearly_summary
      WHERE year >= 2000 AND year <= 2025
      ORDER BY year
    `),

    // ── People leaderboards (Gold) ──────────────────────────────────────────
    queryDatabricks(`
      SELECT person_id, name, film_count,
             avg_rating, total_revenue, avg_popularity,
             rank_film_count, rank_avg_rating, rank_revenue
      FROM workspace.gold.gold_director_leaderboard
      ORDER BY rank_film_count
      LIMIT 50
    `),

    queryDatabricks(`
      SELECT person_id, name, film_count,
             avg_rating, total_revenue, avg_popularity,
             rank_film_count, rank_avg_rating, rank_revenue
      FROM workspace.gold.gold_actor_leaderboard
      ORDER BY rank_film_count
      LIMIT 50
    `),
  ])

  return Response.json({
    topRated,
    topRevenue,
    topPopular,
    genres,
    ratingByGenre,
    yearlyTrends,
    topDirectors,
    topActors,
  })
}