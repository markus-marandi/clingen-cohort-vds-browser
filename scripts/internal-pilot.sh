#!/usr/bin/env bash
# Compatibility entry point; use scripts/browser.sh for browser lifecycle commands.
set -euo pipefail
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/browser.sh" "$@"
