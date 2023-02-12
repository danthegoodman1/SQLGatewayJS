import { SQLGatewayClient } from "./client"

async function main() {
  const client = new SQLGatewayClient({
    url: "http://localhost:8080"
  })

  await client.Ping()

  const res = await client.Query('SELECT $1::INT8 as a_number', [42])
  console.log(JSON.stringify(res, null, 2))

  const t = new Date().getTime()
  await client.Query(`CREATE TABLE if not exists t_${t} (a INT8 PRIMARY KEY)`)

  const tx = await client.Begin()
  await tx.Query(`insert into t_${t} (a) VALUES ($1)`, [t])

  const r1 = await tx.Query(`select * from t_${t}`)
  console.log(JSON.stringify(r1, null, 2))

  await tx.Commit()

  const c2 = await client.Query(`select * from t_${t}`)
  console.log("should be 1 row", JSON.stringify(c2, null, 2))
}

main()
