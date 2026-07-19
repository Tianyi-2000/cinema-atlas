import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  const rows = await queryDatabricks(`
    SELECT cinema_name, distance, version_type, start_time, end_time, show_date
    FROM workspace.silver_cinema_atlas.movieglu_showtimes
    WHERE film_id = ${id}
    ORDER BY show_date, start_time
  `)

  return Response.json({
    date: rows[0]?.show_date ?? null,
    showtimes: rows,
  })
}