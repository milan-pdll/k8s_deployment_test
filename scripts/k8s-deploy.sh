#!/usr/bin/env bash
set -euo pipefail

# scripts/k8s-deploy.sh
# End-to-end deploy script for PGS Search Engine on the 6-node Proxmox HA Kubernetes cluster.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=========================================================="
echo "  Deploying PGS Search Engine to Kubernetes"
echo "=========================================================="

# 1. Run Pre-flight Checks & StorageClass Verification
"${SCRIPT_DIR}/k8s-cluster-prep.sh"

# 2. Ensure secrets.env is ready
"${SCRIPT_DIR}/prepare-secrets.sh"

# 3. Apply manifests via Kustomize
echo ""
echo "==> Applying Kubernetes manifests (kubectl apply -k k8/)..."
kubectl apply -k "${REPO_ROOT}/k8/"

# 4. Wait for bootstrap Jobs
echo ""
echo "==> Waiting for database & Kafka bootstrap jobs to complete..."
kubectl -n pgs-search-engine wait --for=condition=complete job/db-bootstrap --timeout=180s || true
kubectl -n pgs-search-engine wait --for=condition=complete job/kafka-init --timeout=120s || true

# 5. Pod status
echo ""
echo "==> Current cluster workload status:"
kubectl -n pgs-search-engine get pods -o wide

echo ""
echo "=========================================================="
echo "  Deployment initiated!"
echo "=========================================================="
echo "Accessing the Application:"
echo "  1. Internal LAN Access (from college network):"
echo "     http://10.20.62.101:30080  (or any worker IP:30080)"
echo ""
echo "  2. Public Internet Access (via HA Cloudflare Tunnel):"
echo "     Check your tunnel URL with:"
echo "     kubectl -n pgs-search-engine logs -l app.kubernetes.io/name=cloudflared -c cloudflared | grep trycloudflare"
echo ""
echo "  3. Airflow Webserver (ETL Orchestration):"
echo "     kubectl -n pgs-search-engine port-forward svc/airflow-webserver 8080:8080"
echo "     http://localhost:8080 (login credentials in k8/secrets/secrets.env)"
echo ""
echo "Monitor pod rollout live:"
echo "  kubectl -n pgs-search-engine get pods -w"
echo "=========================================================="
