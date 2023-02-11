export class QueryError extends Error {
  constructor (message: string) {
    super(message)
  }
}

export class HighStatusCode extends Error {
  code: number
  response: string
  constructor (message: string, errorCode: number) {
    super(`High status code (${errorCode}): ${message}`)
    this.response = message
    this.code = errorCode
  }
}
