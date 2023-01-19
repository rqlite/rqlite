import httpProxy from "http-proxy";
import { NextApiRequest, NextApiResponse } from "next";

export const config = {
  api: {
    // Enable `externalResolver` option in Next.js
    externalResolver: true,
    bodyParser: false,
  },
};

export default (req: NextApiRequest, res: NextApiResponse) => {
  const rqliteHost = process.env.RQLITE_HOST || "http://127.0.0.1:4001";
  return new Promise((resolve, reject) => {
    const proxy: httpProxy = httpProxy.createProxy();

    proxy
      .once("proxyReq", (proxyReq, req, res, options) => {
        proxyReq.setHeader("Authorization", String(req.headers["x-rqlite-authorization"]));
        if(proxyReq.path && req.url) {
          proxyReq.path = req.url.replace("/api/rqlite", "");
        }
      })
      .once("proxyRes", resolve)
      .once("error", reject)
      .web(req, res, {
        changeOrigin: true,
        target: rqliteHost,
      });
  });
}