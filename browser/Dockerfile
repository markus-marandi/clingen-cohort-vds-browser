FROM docker.io/library/node:20-slim

# pin pnpm 8: the repo's pnpm-lock.yaml is lockfileVersion 6.0, which pnpm 9+
# refuses to read (ERR_PNPM_BROKEN_LOCKFILE). Matches .tool-versions (8.14.3).
RUN npm install -g pnpm@8.14.3

WORKDIR /app

COPY . .

RUN printf 'link-workspace-packages=true\nengine-strict=false\nshamefully-hoist=true\n' > .npmrc \
    && CI=true pnpm install

WORKDIR /app/browser

ENV NODE_ENV=development
# webpack dev server proxies /api to this target from inside the container, so it
# must point at the compose service. The proxy rewrites '^/api' off the request
# path, so the target must carry the /api prefix.
ENV GNOMAD_API_URL=http://graphql-api:8000/api
# webpack dev server port is hardcoded to 8008 in webpack.config.js
EXPOSE 8008

CMD ["pnpm", "start"]
