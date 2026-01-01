#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

ENV_FILE="$ROOT_DIR/.env"
ADDR_OVERRIDE=""
INDEX_OVERRIDE=""

usage() {
	cat <<'EOF'
Usage: ./run-worker.sh [options]

Options:
  --addr ADDR     Override listen address (default derived from WORKER_ADDRS)
  --env FILE      Path to env file (default ./ .env)
  --index N       Override worker index (default WORKER_INDEX in env)
  -h, --help      Show this help
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--addr)
		ADDR_OVERRIDE="$2"
		shift 2
		;;
	--env)
		ENV_FILE="$2"
		shift 2
		;;
	--index)
		INDEX_OVERRIDE="$2"
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

if [[ ! -f "$ENV_FILE" ]]; then
	echo "[worker] Env file not found: $ENV_FILE" >&2
	exit 1
fi

# shellcheck disable=SC2046,SC1090
set -a
source "$ENV_FILE"
set +a

if [[ -n "$INDEX_OVERRIDE" ]]; then
	WORKER_INDEX="$INDEX_OVERRIDE"
fi

if [[ -z "${WORKER_ADDRS:-}" ]]; then
	echo "[worker] WORKER_ADDRS not defined in $ENV_FILE" >&2
	exit 1
fi
if [[ -z "${WORKER_INDEX:-}" ]]; then
	echo "[worker] WORKER_INDEX not defined in $ENV_FILE (or via --index)" >&2
	exit 1
fi
if ! [[ "$WORKER_INDEX" =~ ^[0-9]+$ ]]; then
	echo "[worker] WORKER_INDEX must be numeric, got '$WORKER_INDEX'" >&2
	exit 1
fi

IFS=',' read -r -a ADDRS <<< "$WORKER_ADDRS"
idx="$WORKER_INDEX"
if (( idx < 0 || idx >= ${#ADDRS[@]} )); then
	echo "[worker] WORKER_INDEX $idx out of range (have ${#ADDRS[@]} workers)" >&2
	exit 1
fi

entry="${ADDRS[idx]//[[:space:]]/}"
port="${entry##*:}"
if [[ -z "$port" || "$port" == "$entry" ]]; then
	echo "[worker] Could not parse port from entry '$entry'" >&2
	exit 1
fi

LISTEN_ADDR="${ADDR_OVERRIDE:-":$port"}"

if [[ ! -f "go.mod" ]]; then
	echo "[worker] go.mod not found in $(pwd). Did you extract the bundle correctly?" >&2
	exit 1
fi

BIN_DIR="$ROOT_DIR/bin"
mkdir -p "$BIN_DIR"

echo "[worker] Building worker binary..."
go build -o "$BIN_DIR/golworker" "./cmd/golworker"

if pids=$(pgrep -f "golworker" || true) && [[ -n "$pids" ]]; then
	filtered=()
	for pid in $pids; do
		if [[ "$pid" == "$$" ]]; then
			continue
		fi
		filtered+=("$pid")
	done
	if [[ ${#filtered[@]} -gt 0 ]]; then
		echo "[worker] Found running golworker processes (${filtered[*]}); terminating..."
		kill "${filtered[@]}" 2>/dev/null || true
		sleep 1
		if pids=$(pgrep -f "golworker" || true) && [[ -n "$pids" ]]; then
			filtered=()
			for pid in $pids; do
				if [[ "$pid" == "$$" ]]; then
					continue
				fi
				filtered+=("$pid")
			done
			if [[ ${#filtered[@]} -gt 0 ]]; then
				echo "[worker] Force killing remaining golworker processes (${filtered[*]})..."
				kill -9 "${filtered[@]}" 2>/dev/null || true
			fi
		fi
	fi
fi

echo "[worker] Starting worker on $LISTEN_ADDR (index $idx)..."
exec "$BIN_DIR/golworker" --addr "$LISTEN_ADDR"
