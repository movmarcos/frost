#!/usr/bin/env bash
# ============================================================
# deploy.sh — Run frost (declarative Snowflake DDL manager)
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load .env file if it exists
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    echo "Loading environment variables from .env ..."
    set -a
    source "${SCRIPT_DIR}/.env"
    set +a
else
    echo "ERROR: .env file not found. Copy .env.example to .env and fill in the values."
    exit 1
fi

# Validate required variables
for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_DATABASE SNOWFLAKE_PRIVATE_KEY_PATH; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: ${var} is not set in .env"
        exit 1
    fi
done

# Resolve private key path relative to script directory
if [[ ! "${SNOWFLAKE_PRIVATE_KEY_PATH}" = /* ]]; then
    SNOWFLAKE_PRIVATE_KEY_PATH="${SCRIPT_DIR}/${SNOWFLAKE_PRIVATE_KEY_PATH}"
fi

if [[ ! -f "${SNOWFLAKE_PRIVATE_KEY_PATH}" ]]; then
    echo "ERROR: Private key file not found at ${SNOWFLAKE_PRIVATE_KEY_PATH}"
    exit 1
fi

echo "============================================"
echo "  frost — Snowflake DDL Deployment"
echo "============================================"
echo "  Account:    ${SNOWFLAKE_ACCOUNT}"
echo "  User:       ${SNOWFLAKE_USER}"
echo "  Role:       ${SNOWFLAKE_ROLE:-SYSADMIN}"
echo "  Warehouse:  ${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"
echo "  Database:   ${SNOWFLAKE_DATABASE}"
echo "  Key file:   ${SNOWFLAKE_PRIVATE_KEY_PATH}"
echo "============================================"

# Determine frost command (default: deploy)
FROST_CMD="${1:-deploy}"

# Build the command
CMD=(
    python -m frost
    --config "${SCRIPT_DIR}/frost-config.yml"
    --objects-folder "${SCRIPT_DIR}/objects"
)

# Add FROST_VARS if set
if [[ -n "${FROST_VARS:-}" ]]; then
    CMD+=(--vars "${FROST_VARS}")
fi

# Add verbose flag
if [[ "${2:-}" == "--verbose" || "${1:-}" == "--verbose" ]]; then
    CMD+=(-v)
fi

# Add the sub-command
case "${FROST_CMD}" in
    plan)
        CMD+=(plan)
        ;;
    deploy)
        CMD+=(deploy)
        if [[ "${2:-}" == "--dry-run" ]]; then
            CMD+=(--dry-run)
            echo "  ** DRY RUN MODE **"
        fi
        ;;
    graph)
        CMD+=(graph)
        ;;
    *)
        echo "Usage: $0 {plan|deploy|graph} [--dry-run] [--verbose]"
        exit 1
        ;;
esac

echo ""
echo "Running frost..."
echo ""

"${CMD[@]}"

echo ""
echo "Done."
