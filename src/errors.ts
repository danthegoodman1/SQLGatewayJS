export class QueryError extends Error {
  constructor (message: string, query: string) {
    super(message)
  }
}

export class HighStatusCode extends Error {
  code: number
  response: string
  constructor (message: string, errorCode: number) {
    super(`high status code (${errorCode}): ${message}`)
    this.response = message
    this.code = errorCode
  }
}

export class TxEnded extends Error {
  constructor() {
    super("transaction ended")
  }
}

export class TxNotFound extends Error {
  constructor() {
    super("transaction not found, did it timeout?")
  }
}
