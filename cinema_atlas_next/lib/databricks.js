const HOST  = process.env.DATABRICKS_HOST
const TOKEN = process.env.DATABRICKS_TOKEN
const WH_ID = process.env.DATABRICKS_WAREHOUSE_ID

const HEADERS = {
  "Authorization": `Bearer ${TOKEN}`,
  "Content-Type": "application/json",
}

async function poll(statementId) {
  const url = `https://${HOST}/api/2.0/sql/statements/${statementId}`
  for (let i = 0; i < 60; i++) {
    await new Promise(r => setTimeout(r, 1000))
    const res  = await fetch(url, { headers: HEADERS, cache: "no-store" })
    const data = await res.json()
    const state = data.status?.state
    if (state === "SUCCEEDED") return data
    if (["FAILED", "CANCELED", "CLOSED"].includes(state)) {
      throw new Error(`Query ${state}: ${JSON.stringify(data.status)}`)
    }
  }
  throw new Error("Query timed out after 60s")
}

export async function queryDatabricks(statement) {
  const res = await fetch(
    `https://${HOST}/api/2.0/sql/statements/`,
    {
      method: "POST",
      headers: HEADERS,
      body: JSON.stringify({
        warehouse_id: WH_ID,
        statement,
        wait_timeout: "50s",
        disposition:  "INLINE",
        format:       "JSON_ARRAY",
      }),
      cache: "no-store",
    }
  )

  let data = await res.json()

  if (data.status?.state === "PENDING" || data.status?.state === "RUNNING") {
    data = await poll(data.statement_id)
  }

  if (data.status?.state !== "SUCCEEDED") {
    throw new Error(`Query failed: ${JSON.stringify(data.status)}`)
  }

  const columns = data.manifest.schema.columns.map(c => c.name)
  const rows    = data.result?.data_array || []

  return rows.map(row =>
    Object.fromEntries(columns.map((col, i) => [col, row[i]]))
  )
}

export async function queryDatabricksLarge(statement) {
  const res = await fetch(
    `https://${HOST}/api/2.0/sql/statements/`,
    {
      method: "POST",
      headers: HEADERS,
      body: JSON.stringify({
        warehouse_id: WH_ID,
        statement,
        wait_timeout: "50s",
        disposition:  "EXTERNAL_LINKS",
        format:       "JSON_ARRAY",
      }),
      cache: "no-store",
    }
  )

  let data = await res.json()

  if (data.status?.state === "PENDING" || data.status?.state === "RUNNING") {
    data = await poll(data.statement_id)
  }

  if (data.status?.state !== "SUCCEEDED") {
    throw new Error(`Query failed: ${JSON.stringify(data.status)}`)
  }

  const columns = data.manifest.schema.columns.map(c => c.name)
  const chunks  = data.manifest.chunks || []
  let allRows = []

  for (const chunkInfo of chunks) {
    const linkRes = await fetch(
      `https://${HOST}/api/2.0/sql/statements/${data.statement_id}/result/chunks/${chunkInfo.chunk_index}`,
      { headers: HEADERS, cache: "no-store" }
    )
    const linkData = await linkRes.json()
    const externalUrl = linkData.external_links[0].external_link

    const chunkDataRes = await fetch(externalUrl)
    const rows = await chunkDataRes.json()
    allRows = allRows.concat(rows)
  }

  return allRows.map(row =>
    Object.fromEntries(columns.map((col, i) => [col, row[i]]))
  )
}