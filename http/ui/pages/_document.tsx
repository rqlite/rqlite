import { Html, Head, Main, NextScript } from 'next/document'

export default function Document() {
  const isDev = process.env.NODE_ENV === "development";

  return (
    <Html lang="en">
      <Head>
        {isDev ? (
          // dev mode needs 'unsafe-eval' for script-src
          // https://github.com/vercel/next.js/issues/14221
          <meta
            httpEquiv="Content-Security-Policy"
            content="default-src 'self'; script-src 'self' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; child-src 'none';"
          />
        ) : (
          <meta
            httpEquiv="Content-Security-Policy"
            content="default-src 'self'; style-src 'self' 'unsafe-inline'; child-src 'none';"
          />
        )}
        <link rel="shortcut icon" href="/ui/favicon.ico" />
        <title>rqlite manager</title>
      </Head>
      <body>
        <Main />
        <NextScript />
      </body>
    </Html>
  );
}
