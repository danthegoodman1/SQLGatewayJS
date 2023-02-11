export interface QueryResponse {
  Columns: string[]
  Rows: any[]
  TimeNano?: number
}

export interface ExecResponse {
  TimeNano?: number
}
