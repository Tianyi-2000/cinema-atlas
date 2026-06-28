import { queryDatabricks } from "@/lib/databricks"

export async function GET() {
  const rows = await queryDatabricks(`
    SELECT YEAR(release_date) AS year, COUNT(*) AS films
    FROM workspace.silver.movies
    WHERE release_date IS NOT NULL
      AND vote_count >= 200
    GROUP BY YEAR(release_date)
    ORDER BY year
  `)
  return Response.json(rows)
}