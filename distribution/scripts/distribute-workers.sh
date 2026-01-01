#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"

usage() {
	cat <<'EOF'
Usage: scripts/distribute-workers.sh [options]

Options:
  -e, --env FILE   Path to .env file with WORKER_* entries (default ./.env)
  -h, --help       Show this help

Required .env entries:
  WORKER_ADDRS         Comma-separated list of host:port pairs (ordered)
  WORKER_SSH_USER      SSH user for all worker machines
  WORKER_SSH_KEY       Path to the SSH private key
  WORKER_REMOTE_BASE   Remote directory where bundles will be extracted (e.g. ~/workspace)
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
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

if [[ ! -f "$ENV_FILE" ]]; then
	echo "[distribute] Env file not found: $ENV_FILE" >&2
	exit 1
fi

# shellcheck disable=SC2046,SC1090
ENV_FILE_ABS="$(cd "$(dirname "$ENV_FILE")" && pwd)/$(basename "$ENV_FILE")"
set -a
source "$ENV_FILE_ABS"
set +a
ENV_FILE="$ENV_FILE_ABS"

if [[ -z "${WORKER_ADDRS:-}" ]]; then
	echo "[distribute] WORKER_ADDRS not set in $ENV_FILE" >&2
	exit 1
fi
if [[ -z "${WORKER_SSH_USER:-}" ]]; then
	echo "[distribute] WORKER_SSH_USER not set in $ENV_FILE" >&2
	exit 1
fi
if [[ -z "${WORKER_SSH_KEY:-}" ]]; then
	echo "[distribute] WORKER_SSH_KEY not set in $ENV_FILE" >&2
	exit 1
fi

REMOTE_BASE="${WORKER_REMOTE_BASE:-~/workspace}"
if [[ "$REMOTE_BASE" == "~" ]]; then
	REMOTE_BASE="/home/${WORKER_SSH_USER}"
elif [[ "$REMOTE_BASE" == ~/* ]]; then
	REMOTE_BASE="/home/${WORKER_SSH_USER}/${REMOTE_BASE:2}"
fi
SSH_KEY="${WORKER_SSH_KEY/#\~/$HOME}"
REMOTE_DIR="${REMOTE_BASE%/}/golworker"

TMP_DIR="$(mktemp -d)"
cleanup() {
	rm -rf "$TMP_DIR"
}
trap cleanup EXIT

STAGE="$TMP_DIR/golworker"
mkdir -p "$STAGE/cmd"

cp "$REPO_ROOT/go.mod" "$REPO_ROOT/go.sum" "$STAGE/"
cp -R "$REPO_ROOT/cmd/golworker" "$STAGE/cmd/"
cp -R "$REPO_ROOT/gol" "$STAGE/gol"
cp -R "$REPO_ROOT/util" "$STAGE/util"
install -m 755 "$REPO_ROOT/scripts/run-worker-local.sh" "$STAGE/run-worker.sh"
cp "$ENV_FILE" "$STAGE/.env"

ARCHIVE="$TMP_DIR/golworker.tar.gz"
echo "[distribute] Creating archive $ARCHIVE"
COPYFILE_DISABLE=1 tar --disable-copyfile --no-xattrs --no-acls -czf "$ARCHIVE" -C "$TMP_DIR" golworker

IFS=',' read -r -a WORKER_LIST <<< "$WORKER_ADDRS"
for i in "${!WORKER_LIST[@]}"; do
	entry="${WORKER_LIST[i]//[[:space:]]/}"
	host="${entry%%:*}"
	if [[ -z "$host" ]]; then
		echo "[distribute] Unable to extract host from entry '$entry'" >&2
		exit 1
	fi
	target="${WORKER_SSH_USER}@${host}"
	echo "[distribute] Deploying package to $target"
	ssh -i "$SSH_KEY" "$target" "rm -rf '${REMOTE_DIR}' && mkdir -p '${REMOTE_BASE}'"
	scp -i "$SSH_KEY" "$ARCHIVE" "$target:${REMOTE_BASE}/golworker.tar.gz"
	ssh -i "$SSH_KEY" "$target" "cd '${REMOTE_BASE}' && tar -xzf golworker.tar.gz && rm golworker.tar.gz && chmod +x '${REMOTE_DIR}/run-worker.sh' && printf '\nWORKER_INDEX=%s\n' ${i} >> '${REMOTE_DIR}/.env'"
done

echo "[distribute] Package deployed to ${#WORKER_LIST[@]} worker(s)."
