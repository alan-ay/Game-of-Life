#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BROKER_ADDR="localhost:7000"
HEADLESS="false"
RESUME="false"
IMAGE_SIZE=""
TURN_OVERRIDE=""
EXTRA_ARGS=()

usage() {
	cat <<'EOF'
Usage: scripts/start-controller.sh [options] [-- extra gol args]

Options:
  -b, --broker ADDR      Broker RPC address (default localhost:7000)
      --headless         Run without SDL window
      --resume           Attempt to resume the last paused distributed session
      --size WxH         Override default image width/height (e.g. 1024x512)
      --turns N          Override number of turns processed by controller
  -h, --help             Show this help

All arguments after -- are passed directly to the Go controller.
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	-b|--broker)
		BROKER_ADDR="$2"
		shift 2
		;;
	--headless)
		HEADLESS="true"
		shift
		;;
	--resume)
		RESUME="true"
		shift
		;;
	--size)
		IMAGE_SIZE="$2"
		shift 2
		;;
	--turns)
		TURN_OVERRIDE="$2"
		shift 2
		;;
	-h|--help)
		usage
		exit 0
		;;
	--)
		shift
		EXTRA_ARGS+=("$@")
		break
		;;
	*)
		echo "Unknown option: $1" >&2
		usage
		exit 1
		;;
	esac
done

echo "[controller] Building binary..."
mkdir -p "$REPO_ROOT/bin"
go build -o "$REPO_ROOT/bin/gol" "$REPO_ROOT"

CMD=("$REPO_ROOT/bin/gol" -worker "$BROKER_ADDR")
if [[ "$HEADLESS" == "true" ]]; then
	CMD+=(--headless)
fi
if [[ "$RESUME" == "true" ]]; then
	CMD+=(--resume)
fi
if [[ -n "$IMAGE_SIZE" ]]; then
	if [[ "$IMAGE_SIZE" =~ ^([0-9]+)x([0-9]+)$ ]]; then
		CMD+=(-w "${BASH_REMATCH[1]}" -h "${BASH_REMATCH[2]}")
	else
		echo "[controller] Invalid --size value '$IMAGE_SIZE' (expected WxH)" >&2
		exit 1
	fi
fi
if [[ -n "$TURN_OVERRIDE" ]]; then
	if [[ "$TURN_OVERRIDE" =~ ^[0-9]+$ ]]; then
		CMD+=(-turns "$TURN_OVERRIDE")
	else
		echo "[controller] Invalid --turns value '$TURN_OVERRIDE' (expected integer)" >&2
		exit 1
	fi
fi
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
	CMD+=("${EXTRA_ARGS[@]}")
fi

echo "[controller] Starting: ${CMD[*]}"
exec "${CMD[@]}"
