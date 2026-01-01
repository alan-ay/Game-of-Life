#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ADDR=":7000"
WORKERS=""
ENV_FILE="$REPO_ROOT/.env"

usage() {
	cat <<'EOF'
Usage: scripts/start-broker.sh [options]

Options:
  -a, --addr ADDR       Broker listen address (default :7000)
  -w, --workers LIST    Comma-separated worker addresses (overrides env file)
  -e, --env FILE        Path to .env file with WORKER_ADDRS (default ./.env)
  -h, --help            Show this help

Examples:
	bash scripts/start-broker.sh --env .env.production --addr :8080
	bash scripts/start-broker.sh -w "10.0.0.1:8123,10.0.0.2:8123" --addr :8080
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	-a|--addr)
		ADDR="$2"
		shift 2
		;;
	-w|--workers)
		WORKERS="$2"
		shift 2
		;;
	-e|--env)
		ENV_FILE="$2"
		shift 2
		;;
	-h|--help)
		usage
		exit 0
		;;
	*)
		echo "Unknown option: $1" >&2
		usage
		exit 1
		;;
	esac
done

echo "[broker] Building binary..."
mkdir -p "$REPO_ROOT/bin"
go build -o "$REPO_ROOT/bin/broker" "$REPO_ROOT/cmd/broker"

CMD=("$REPO_ROOT/bin/broker" -addr "$ADDR")
if [[ -n "$WORKERS" ]]; then
	CMD+=(-workers "$WORKERS")
else
	CMD+=(-workers-env "$ENV_FILE")
fi

echo "[broker] Starting: ${CMD[*]}"
exec "${CMD[@]}"
