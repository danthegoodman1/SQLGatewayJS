import { SQLGatewayClient } from "./client"

export interface SQLGatewayTransactionConfig {
  client: SQLGatewayClient
  timeoutSeconds?: number
}

export class SQLGatewayTransaction {
  transactionID: string | undefined
  client: SQLGatewayClient
  timeoutSeconds?: number
  constructor(config: SQLGatewayTransactionConfig) {
    this.client = config.client
  }

  /**
   * Begin returns the transaction ID
   */
  async begin() {
    const res = await this.client.makeRequest("/begin", "POST", {}, {
      TxTimeoutSec: this.timeoutSeconds
    })
    const resBody = await res.json() as {
      TxID: string
    }
    this.transactionID = resBody.TxID
  }

  async Query(statement: string | string[], params: any[] | any[][]) {
    return this.client.query(statement, params, {
      txKey: this.transactionID
    })
  }

  async Exec(statement: string | string[], params: any[] | any[][]) {
    const queryRes = await this.client.query(statement, params, {
      exec: true,
      txKey: this.transactionID
    })
    return {
      TimeNano: Array.isArray(queryRes) ? queryRes[0].TimeNano : queryRes.TimeNano
    }
  }

  async Commit() {
    const res = await this.client.makeRequest("/commit", "POST", {}, {
      TxID: this.transactionID
    })
  }

  async Rollback() {
    const res = await this.client.makeRequest("/rollback", "POST", {}, {
      TxID: this.transactionID
    })
  }
}
