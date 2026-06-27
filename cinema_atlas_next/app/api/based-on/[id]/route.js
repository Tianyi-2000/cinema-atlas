import { queryDatabricks } from "@/lib/databricks"

export async function GET(request, { params }) {
  const { id } = await params

  const rows = await queryDatabricks(`
    WITH film_tconst AS (
      SELECT tconst FROM workspace.silver.films WHERE id = ${id} LIMIT 1
    ),
    sources AS (
      SELECT wb.source_wikidata_id, wb.source_label
      FROM workspace.bronze.wikidata_based_on wb
      JOIN film_tconst ft ON wb.imdb_id = ft.tconst
    )
    SELECT
      s.source_wikidata_id,
      s.source_label,
      f.id   AS film_id,
      f.title,
      f.year
    FROM sources s
    JOIN workspace.bronze.wikidata_based_on wb2
      ON s.source_wikidata_id = wb2.source_wikidata_id
    JOIN workspace.silver.films f ON wb2.imdb_id = f.tconst
    JOIN film_tconst ft ON wb2.imdb_id != ft.tconst
    ORDER BY s.source_label, f.year
    LIMIT 60
  `)

  // Group by source
  const bySource = {}
  for (const row of rows) {
    const sid = row.source_wikidata_id
    if (!bySource[sid]) {
      bySource[sid] = {
        source_wikidata_id: sid,
        source_label: row.source_label,
        coAdaptedFilms: [],
      }
    }
    bySource[sid].coAdaptedFilms.push({
      film_id: row.film_id,
      title: row.title,
      year: row.year,
    })
  }

  return Response.json(Object.values(bySource))
}
