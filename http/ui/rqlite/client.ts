import axios from "axios";
import {
  QueryResults,
  ReadResult,
  WriteResult,
} from "@/types/rqlite";
import { getConfig } from "hooks/useConfig";

const httpClient = axios.create({
  baseURL: process.env.NODE_ENV === "development"
    ? "/api/rqlite"
    : "",
});

httpClient.interceptors.request.use(function (config) {
  const activeAccount = getConfig().accounts.find((u) => u.isActive);

  if (activeAccount) {
    const { username, password } = activeAccount;
    config.headers = config.headers ?? {};
    const authHeader = btoa(username + ":" + password);
    (config.headers as any)["Authorization"] = `Basic ${authHeader}`;
    (config.headers as any)["X-Rqlite-Authorization"] = `Basic ${authHeader}`;
  }
  return config;
});

export const rqlite = {
  read: (q: string[]) =>
    httpClient.post<QueryResults<ReadResult>>("/db/query?timings", q),
  write: (q: string[]) =>
    httpClient.post<QueryResults<WriteResult>>("/db/execute?timings", q),
  status: () => httpClient.get<any>("/status"),
  nodes: () => httpClient.get<any>("/nodes"),
  readiness: () => httpClient.get<any>("/readyz"),
  expvar: () => httpClient.get<any>("/debug/vars"),
};
