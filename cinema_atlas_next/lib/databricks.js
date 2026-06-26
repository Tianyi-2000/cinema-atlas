const HOST  = process.env.DATABRICKS_HOST
const TOKEN = process.env.DATABRICKS_TOKEN
const WH_ID = process.env.DATABRICKS_WAREHOUSE_ID

export async function queryDatabricks(statement) {
  const res = await fetch(
    `https://${HOST}/api/2.0/sql/statements/`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        warehouse_id: WH_ID,
        statement,
        wait_timeout: "30s",
        disposition: "INLINE",
        format: "JSON_ARRAY",
      }),
      cache: "no-store",
    }
  )

  const data = await res.json()

  if (data.status?.state !== "SUCCEEDED") {
    throw new Error(`Query failed: ${JSON.stringify(data.status)}`)
  }

  const columns = data.manifest.schema.columns.map(c => c.name)
  const rows    = data.result?.data_array || []

  return rows.map(row =>
    Object.fromEntries(columns.map((col, i) => [col, row[i]]))
  )
}