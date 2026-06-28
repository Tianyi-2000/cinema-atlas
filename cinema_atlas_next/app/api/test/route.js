import { queryDatabricks } from "@/lib/databricks"

export async function GET() {
  try {
    const rows = await queryDatabricks(
      "SELECT COUNT(*) AS films FROM workspace.silver.movies"
    )
    return Response.json(rows[0])
  } catch (e) {
    return Response.json({ error: e.message }, { status: 500 })
  }
}