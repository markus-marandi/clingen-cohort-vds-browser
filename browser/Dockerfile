FROM docker.io/library/node:20-slim

RUN npm install -g pnpm

WORKDIR /app

COPY . .

RUN printf 'link-workspace-packages=true\nengine-strict=false\nshamefully-hoist=true\n' > .npmrc \
    && CI=true pnpm install

WORKDIR /app/browser

ENV NODE_ENV=development
ENV GNOMAD_API_URL=http://localhost:8000
EXPOSE 3000

CMD ["pnpm", "start"]
