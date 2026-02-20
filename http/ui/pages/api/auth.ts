import type { NextApiRequest, NextApiResponse } from 'next'

export default function handler(_: NextApiRequest, res: NextApiResponse) {
  res.statusCode = 401;
  res.end(`Auth Required.`)
}