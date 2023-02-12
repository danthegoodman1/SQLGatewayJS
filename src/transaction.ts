import { SQLGatewayClient } from "./client"
import { HighStatusCode, QueryError, TxEnded, TxNotFound } from "./errors"

export interface SQLGatewayTransactionConfig {
  client: SQLGatewayClient
  timeoutSeconds?: number
}

export class SQLGatewayTransaction {
  transactionID: string | undefined
  client: SQLGatewayClient
  timeoutSeconds?: number
  ended = false
  constructor(config: SQLGatewayTransactionConfig) {
    this.client = config.client
  }

  /**
   * Begin returns the transaction ID
   */
  async begin() {
    if (this.transactionID) {
      // No-op
      return
    }
    const res = await this.client.makeRequest("/psql/begin", "POST", {}, {
      TxTimeoutSec: this.timeoutSeconds
    })
    const resBody = await res.json() as {
      TxID: string
    }
    this.transactionID = resBody.TxID
  }

  async Query(statement: string | string[], params?: any[] | any[][]) {
    if (this.ended) {
      throw new TxEnded()
    }
    try {
      const res = this.client.query(statement, params, {
        txKey: this.transactionID
      })
      return res
    } catch (error) {
      if (error instanceof HighStatusCode && error.code === 404) {
        throw new TxNotFound()
      }
      if (error instanceof QueryError) {
        this.ended = true
      }
      throw error
    }
  }

  async Exec(statement: string | string[], params: any[] | any[][]) {
    if (this.ended) {
      throw new TxEnded()
    }
    try {
      const queryRes = await this.client.query(statement, params, {
        exec: true,
        txKey: this.transactionID
      })
      return {
        TimeNano: Array.isArray(queryRes) ? queryRes[0].TimeNano : queryRes.TimeNano
      }
    } catch (error) {
      if (error instanceof HighStatusCode && error.code === 404) {
        throw new TxNotFound()
      }
      if (error instanceof QueryError) {
        this.ended = true
      }
      throw error
    }
  }

  async Commit() {
    if (this.ended) {
      throw new TxEnded()
    }
    try {
      await this.client.makeRequest("/psql/commit", "POST", {}, {
        TxID: this.transactionID
      })
      this.ended = true
    } catch (error) {
      if (error instanceof HighStatusCode && error.code === 404) {
        throw new TxNotFound()
      }
      throw error
    }
  }

  async Rollback() {
    if (this.ended) {
      throw new TxEnded()
    }
    try {
      await this.client.makeRequest("/psql/rollback", "POST", {}, {
        TxID: this.transactionID
      })
      this.ended = true
    } catch (error) {
      if (error instanceof HighStatusCode && error.code === 404) {
        throw new TxNotFound()
      }
      throw error
    }
  }
}
