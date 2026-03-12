FROM node:20-slim

RUN npm install -g pnpm

WORKDIR /app

# copy the full workspace so pnpm can resolve all local @gnomad/* packages
COPY . .

# link-workspace-packages resolves @gnomad/* from the local workspace ("*" version)
# shamefully-hoist avoids virtual-store permission issues in the container layer
# CI=true tells pnpm it's non-interactive so it won't prompt before removing modules
RUN printf 'link-workspace-packages=true\nengine-strict=false\nshamefully-hoist=true\n' > .npmrc \
    && CI=true pnpm install

WORKDIR /app/graphql-api

ENV NODE_ENV=development
EXPOSE 8000

CMD ["pnpm", "ts-node", "./src/app.ts"]
