import { SQLGatewayTransaction } from "./transaction";
import fetch from 'isomorphic-fetch';
import { ExecResponse, QueryResponse } from "./query";
import { QueryError } from "./errors";

export interface SQLGatewayClientConfig {
  url: string
  username?: string
  password?: string
}

interface queryReq {
  Statement: string
  Params: any[]
  IgnoreCache?: boolean
  ForceCache?: boolean
  Exec?: boolean
  TxKey?: string
}

interface queryRes {
  Columns: any[][]
  Rows: any[][]
  Error?: string
  TimeNS?: number
}

export class SQLGatewayClient {
  url: string
  username?: string
  password?: string

  constructor(config: SQLGatewayClientConfig){
    this.url = config.url
  }

  async Ping() {
    // health check a sqlgateway pod
  }

  async Query(statement: string | string[], params: any[] | any[][]): Promise<QueryResponse | QueryResponse[]> {
    return this.query(statement, params)
  }

  async query(statement: string | string[], params: any[] | any[][], opts?: {exec?: boolean, txKey?: string}): Promise<QueryResponse | QueryResponse[]> {
    const reqBody: queryReq[] = []
    const singleStatement = typeof statement === 'string'
    if (singleStatement) {
      // Single query
      reqBody.push({
        Params: params as any[],
        Statement: statement,
      })
    } else {
      // Array
      for (let i = 0; i < statement.length; i++) {
        reqBody.push({
          Statement: statement[i],
          Params: params[i]
        })
      }
    }

    const res = await this.makeRequest("/query", "POST", {}, reqBody)
    const resBody = await res.json() as queryRes
    if (resBody.Error) {
      throw new QueryError(resBody.Error)
    }

    if (singleStatement) {
      return {
        Columns: resBody.Columns[0],
        Rows: resBody.Columns[0],
        TimeNano: resBody.TimeNS
      } as QueryResponse
    }
    const qr: QueryResponse[] = []
    for (let i = 0; i < resBody.Columns.length; i++) {
      qr.push({
        Columns: resBody.Columns[i],
        Rows: resBody.Rows[i],
        TimeNano: resBody.TimeNS
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
  async makeRequest(endpoint: string, method: string, headers: { [key: string]: string }, body: any) {
    return fetch(this.url+endpoint, {
      method: method,
      headers: {
        "content-type": "application/json",
        "Authorization": this.username && this.password ? Buffer.from(`${this.username}:${this.password}`).toString("base64") : undefined,
        ...headers
      },
      body: JSON.stringify(body)
    })
  }
}
