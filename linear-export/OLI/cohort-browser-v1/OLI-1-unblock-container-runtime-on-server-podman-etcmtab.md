# OLI-1: Unblock container runtime on server (Podman /etc/mtab)

- Status: In Progress
- Priority: Urgent
- Estimate: 3
- Assignee: Markus Mar
- Labels: infra, blocker
- Created: 2026-08-27
- Updated: 2026-08-27
- Linear: https://linear.app/necto-tech/issue/OLI-1/unblock-container-runtime-on-server-podman-etcmtab

## Description

BLOCKER for every downstream deploy task. Server runs Podman 5.5.1; docker compose fails on the /etc/mtab symlink. Resolve by fixing the symlink, switching to `podman compose`, or documenting a Docker-only requirement. Tracked in browser/TODO.md > Local Stack.

## Comments

### Markus Mar — 2026-08-27T14:13:59Z

Repo-side work done (2026-08-27). VM verification still pending — oligo-VM (172.17.91.53) is unreachable without the UT VPN.

**Changes**
- `scripts/stack.sh` — new runtime-agnostic entry point (`doctor|up|down|logs|ps|build|restart|pull|compose`). Resolves docker/podman and the compose implementation on every run instead of once at setup time, and preflights the host before touching containers: /etc/mtab (Linux only), podman graphRoot/runRoot writability, fuse-overlayfs presence when storage.conf sets mount_program, TMPDIR, and port conflicts with the native services from DEMO.md.
- `browser/docker-compose.yml`, `browser/Dockerfile` — fully qualified image names (docker.io/library/redis:7-alpine, docker.io/library/node:20-slim), so podman does not need unqualified-search-registries.
- `setup.sh` — next-steps now point at scripts/stack.sh.
- `DEMO.md` — compose path documented as the alternative to the native startup, with the note that compose publishes the browser on 3000 rather than 8008.
- `browser/TODO.md` — Local Stack section updated.

**Verified locally (macOS, Docker 24.0.5)**: `stack.sh doctor` passes, `docker compose config` validates the compose file.

**Remaining, on the VM**
1. `./scripts/stack.sh doctor` — read the /etc/mtab verdict.
2. If dangling or missing: `sudo ln -sf /proc/self/mounts /etc/mtab`.
3. `./scripts/stack.sh up`, then confirm ES/redis/api/browser come up.

If root is not available on that host, the fallback is documenting the native DEMO.md path as the supported deployment and treating compose as dev-only.
