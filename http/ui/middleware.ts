// middleware.ts
import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

export function middleware(req: NextRequest) {
  const { RQMAN_USERNAME, RQMAN_PASSWORD } = process.env;
  if (!RQMAN_PASSWORD || !RQMAN_USERNAME) {
    return NextResponse.next()
  }
  
  const authHeader = req.headers.get('authorization')
  const url = req.nextUrl
  if (authHeader) {
    const authValue = authHeader.split(' ')[1]
    const [username, password] = atob(authValue).split(':')

    if (username === RQMAN_USERNAME && password === RQMAN_PASSWORD) {
      return NextResponse.next()
    }
  }
  url.pathname = '/api/auth'

  return NextResponse.rewrite(url)
}

export const config = {
  matcher: '/:path*',
}