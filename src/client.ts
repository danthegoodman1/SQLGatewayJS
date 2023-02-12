import { SQLGatewayTransaction } from "./transaction";
import fetch from 'cross-fetch';
import { ExecResponse, QueryResponse } from "./query";
import { HighStatusCode, QueryError } from "./errors";

export interface SQLGatewayClientConfig {
  url: string
  username?: string
  password?: string
}

interface queryReq {
  Queries: {
    Statement: string
    Params?: any[]
    IgnoreCache?: boolean
    ForceCache?: boolean
    Exec?: boolean
  }[]
  TxID?: string
}

interface queryRes {
  Queries: {
    Columns: any[]
    Rows: any[][]
    Error?: string
    TimeNS?: number
  }[]
}

export class SQLGatewayClient {
  url: string
  username?: string
  password?: string

  constructor(config: SQLGatewayClientConfig){
    this.url = config.url
  }

  /**
   * Requests the `/hc` endpoint on a SQLGateway node
   */
  async Ping() {
    await this.makeRequest("/hc", "GET", {})
  }

  async Query(statement: string | string[], params?: any[] | any[][]): Promise<QueryResponse | QueryResponse[]> {
    return this.query(statement, params)
  }

  async query(statement: string | string[], params?: any[] | any[][], opts?: {exec?: boolean, txKey?: string}): Promise<QueryResponse | QueryResponse[]> {
    const reqBody: queryReq = {
      Queries: [],
      TxID: opts?.txKey
    }
    const singleStatement = typeof statement === 'string'
    if (singleStatement) {
      // Single query
      reqBody.Queries.push({
        Params: params as any[],
        Statement: statement,
      })
    } else {
      // Array
      for (let i = 0; i < statement.length; i++) {
        reqBody.Queries.push({
          Statement: statement[i],
          Params: params ? params[i] : undefined,
        })
      }
    }

    const res = await this.makeRequest("/psql/query", "POST", {}, reqBody)
    const resBody = await res.json() as queryRes

    if (singleStatement) {
      if (resBody.Queries[0].Error) {
        throw new QueryError(resBody.Queries[0].Error, statement)
      }
      return {
        Columns: resBody.Queries[0].Columns,
        Rows: resBody.Queries[0].Rows,
        TimeNano: resBody.Queries[0].TimeNS
      } as QueryResponse
    }
    const qr: QueryResponse[] = []
    for (let i = 0; i < resBody.Queries.length; i++) {
      if (resBody.Queries[i].Error) {
        throw new QueryError(resBody.Queries[i].Error!, statement[i])
      }
      qr.push({
        Columns: resBody.Queries[i].Columns,
        Rows: resBody.Queries[i].Rows,
        TimeNano: resBody.Queries[i].TimeNS
      })
    }
    return qr
  }

  async Exec(statement: string | string[], params: any[] | any[][]): Promise<ExecResponse> {
    const queryRes = await this.query(statement, params, {
      exec: true
    })
    return {
      TimeNano: Array.isArray(queryRes) ? queryRes[0].TimeNano : queryRes.TimeNano
    }
  }

  async Begin(): Promise<SQLGatewayTransaction> {
    const tx = new SQLGatewayTransaction({
      client: this
    })
    await tx.begin() // start the transaction
    return tx
  }

  /**
   * @param headers Overwrites pre-made headers
   */
  async makeRequest(endpoint: string, method: string, headers: { [key: string]: string }, body?: any) {
    const hdrs = {
      "content-type": "application/json",
      ...headers
    } as Record<string, string>
    if (this.username && this.password) {
      hdrs["Authorization"] = Buffer.from(`${this.username}:${this.password}`).toString("base64")
    }
    const res = await fetch(this.url+endpoint, {
      method: method,
      headers: hdrs,
      body: method === "GET" ? undefined : JSON.stringify(body)
    })
    if (res.status >= 300) {
      throw new HighStatusCode(await res.text(), res.status)
    }
    return res
  }
}
