#!/usr/bin/env bash
# ============================================================
# generate_key_pair.sh - Generate RSA key pair for Snowflake
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEYS_DIR="${SCRIPT_DIR}/keys"

mkdir -p "${KEYS_DIR}"

echo "============================================"
echo "  Snowflake RSA Key Pair Generator"
echo "============================================"
echo ""

read -rp "Encrypt the private key with a passphrase? (y/N): " USE_PASSPHRASE

PRIVATE_KEY="${KEYS_DIR}/rsa_key.p8"
PUBLIC_KEY="${KEYS_DIR}/rsa_key.pub"

if [[ "${USE_PASSPHRASE,,}" == "y" ]]; then
    echo "Generating encrypted RSA 2048-bit key pair..."
    openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -out "${PRIVATE_KEY}"
else
    echo "Generating unencrypted RSA 2048-bit key pair..."
    openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out "${PRIVATE_KEY}"
fi

# Extract the public key
openssl rsa -in "${PRIVATE_KEY}" -pubout -out "${PUBLIC_KEY}"

# Set restrictive permissions
chmod 600 "${PRIVATE_KEY}"
chmod 644 "${PUBLIC_KEY}"

echo ""
echo "Keys generated successfully:"
echo "  Private key: ${PRIVATE_KEY}"
echo "  Public key:  ${PUBLIC_KEY}"
echo ""

# Extract the public key value for Snowflake (strip header/footer)
PUB_KEY_VALUE=$(grep -v "PUBLIC KEY" "${PUBLIC_KEY}" | tr -d '\n')

echo "============================================"
echo "Run this SQL in Snowflake to assign the public key to your user:"
echo "============================================"
echo ""
echo "  ALTER USER <your_username> SET RSA_PUBLIC_KEY='${PUB_KEY_VALUE}';"
echo ""
echo "============================================"
