export interface ReadResult {
  columns: string[];
  types: string[];
  values?: any[][];
  time: number;
  error: string;
}

export interface WriteResult {
  last_insert_id: number;
  rows_affected: number;
}

export interface TimeOnlyResult {
  time: number;
}

export interface ErrorResult {
  error: string;
}

type AnyResult = ReadResult | WriteResult | TimeOnlyResult | ErrorResult;

export const isReadResult = (result: AnyResult): result is ReadResult => {
  return "columns" in result;
}

export const isWriteResult = (result: AnyResult): result is WriteResult => {
  return "rows_affected" in result;
}

export const isTimeOnlyResult = (result: AnyResult): result is TimeOnlyResult => {
  return "time" in result;
}

export const isErrorResult = (result: AnyResult): result is ErrorResult => {
  return "error" in result;
}

export interface QueryResults<T = (ReadResult | WriteResult | ErrorResult)> {
  results: T[];
  time: number;
}
