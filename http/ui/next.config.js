/** @type {import('next').NextConfig} */
const isProd = process.env.NODE_ENV === "production";
const option = isProd ? {
  basePath: "/ui",
  trailingSlash: true,
} : {};

const nextConfig = {
  reactStrictMode: true,
  output: "standalone",
  ...option,
}

module.exports = nextConfig
