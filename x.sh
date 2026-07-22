#!/usr/bin/env bash
# Run a cargo command inside the dev container — the Linux counterpart of x.ps1.
#   ./x.sh check
#   ./x.sh test
#   ./x.sh run -- --mongo-uri mongodb://mongo:27017
set -euo pipefail

if ! docker compose ps --status running --services 2>/dev/null | grep -qx dev; then
    echo "starting dev environment..."
    docker compose up -d
fi

exec docker compose exec -T dev cargo "$@"
