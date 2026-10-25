# Browser overlay

The cohort browser is a tracked overlay on top of a pinned upstream
`gnomad-browser` checkout. `setup.sh` clones the pinned base and copies the
overlay trees in `browser/` into their matching upstream paths. The overlay is
not a clone of the fork branch, and it does not copy `.git`, `node_modules`,
caches, logs, or genomic data.

## Pinned revisions

| Item | Value |
| --- | --- |
| Upstream base | `db245371f1e572106bda8481ef0f9a6baac6c00d` (`broadinstitute/gnomad-browser` `main`) |
| Fork source | `aa552d5a547404bab767b448322ce08d38d6048b` (`markus-marandi/gnomad-browser` `add-cohort-dataset`) |
| Fork remote | `https://github.com/markus-marandi/gnomad-browser.git` |
| Parent remote | `https://github.com/markus-marandi/clingen-cohort-vds-browser.git` |

At the start of this reconciliation the parent checkout was clean on `main` at
`d776a0662c90829885461799cfe7c830bcb358c2`. The nested fork checkout was
clean on `add-cohort-dataset` at `aa552d5a...`; its `fork/main` was
`db245371...`, which is also the current `origin/main` base used by the overlay.

## Overlay mapping

| Overlay path | Upstream destination |
| --- | --- |
| `browser/browser-src/**` | `browser/src/**` |
| `browser/graphql-api/**` | `graphql-api/**` |
| `browser/dataset-metadata/**` | `dataset-metadata/**` |
| `browser/data-pipeline/**` | `data-pipeline/**` |
| `browser/docker-compose.yml` | `docker-compose.yml` |
| `browser/Dockerfile` | `browser/Dockerfile` |
| `browser/.dockerignore` | `.dockerignore` |

Keeping whole trees means source files, tests, fixtures, and snapshots are applied
together. In particular, the overlay includes the cohort dataset in the shared
metadata and the test/snapshot updates for the default dataset, dataset selector,
variant tables, mitochondrial region tables, feedback links, and variant export.

## Apply and validate

From the parent repository root:

```bash
./setup.sh
```

On a machine without the VM package paths, override the cache locations:

```bash
CLINGEN_PKG_ROOT=/path/to/packages CLINGEN_TMP_DIR=/path/to/tmp ./setup.sh
```

For a fresh reproducible browser checkout without running the full setup script:

```bash
git clone https://github.com/broadinstitute/gnomad-browser.git /tmp/gnomad-browser
git -C /tmp/gnomad-browser checkout db245371f1e572106bda8481ef0f9a6baac6c00d

# Copy the overlay trees using the same mapping as setup.sh, then:
cd /tmp/gnomad-browser
corepack prepare pnpm@8.14.3 --activate
pnpm install --frozen-lockfile
pnpm ts-node ./browser/build/buildHelp.ts
pnpm typecheck
pnpm lint:js
pnpm lint:css
pnpm prettier --check "browser/**/*.{js,jsx,ts,tsx,json,md}" --ignore-path .gitignore
pnpm test:jest --runInBand
(cd browser && pnpm build)
```

`make validate-browser` runs the browser CI checks as a single target. The
`pnpm-lock.yaml` from the pinned base is kept; setup does not delete it and
installs with `--frozen-lockfile` using pnpm 8.14.3.

## Intentional differences from the fork

The tracked overlay is deliberately not a copy of the fork checkout:

- The overlay is file-mapped and pinned to `db245371...`; it is not an unpinned
  clone of `add-cohort-dataset`.
- `browser/src/Query.tsx` is not overlaid. The pinned base already contains the
  `X-GnomAD-Client` request header there; the overlay only needs to restore it in
  `browser/src/search.ts`, where the parent overlay had dropped it.
- Parent-only setup files (Dockerfiles, Compose file, `.dockerignore`) are kept in
  the overlay and are identical to the validated fork versions.
- `browser/graphql-api/Dockerfile` lives at the destination path so the overlay
  tree can be copied directly to `graphql-api/Dockerfile`.
- The parent's newer cohort functionality is preserved. The overlaid
  `CohortVariantPage.tsx`, GraphQL variant/cohort types, cohort queries, and
  `CohortFilterPage.tsx` retain the full annotation field set, direct gene-symbol
  queries, multi-gene summaries, combined ClinVar/consequence/impact/region/REVEL
  filters, and cohort dataset routing.
- `dataset-metadata/metadata.ts` uses the pinned base's v4.1.2 labels plus the
  `cohort` label, rather than the older v4.1.0 labels that were in the previous
  parent overlay.
