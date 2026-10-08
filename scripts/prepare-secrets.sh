#!/usr/bin/env bash
set -euo pipefail

# scripts/prepare-secrets.sh
# Generates k8/secrets/secrets.env from template with secure random keys if not already present.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SECRETS_DIR="${REPO_ROOT}/k8/secrets"
ENV_FILE="${SECRETS_DIR}/secrets.env"
EXAMPLE_FILE="${SECRETS_DIR}/secrets.env.example"

if [ -f "${ENV_FILE}" ]; then
  echo "==> k8/secrets/secrets.env already exists. Preserving existing secrets."
  exit 0
fi

echo "==> Creating k8/secrets/secrets.env from secrets.env.example..."
cp "${EXAMPLE_FILE}" "${ENV_FILE}"

# Generate cryptographically secure random secrets
GEN_API_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))" 2>/dev/null || openssl rand -base64 32 | tr -d '\n')
GEN_AIRFLOW_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(16))" 2>/dev/null || openssl rand -hex 16 | tr -d '\n')

sed -i "s/change-me-to-a-long-random-string-0123456789/${GEN_API_SECRET}/g" "${ENV_FILE}"
sed -i "s/dummy_dev_secret_change_me/${GEN_AIRFLOW_SECRET}/g" "${ENV_FILE}"

echo "==> Successfully generated k8/secrets/secrets.env with random API_AUTH_SECRET and AIRFLOW_SECRET_KEY."
