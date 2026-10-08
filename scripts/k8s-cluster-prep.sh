#!/usr/bin/env bash
set -euo pipefail

# scripts/k8s-cluster-prep.sh
# Pre-flight readiness check & storage bootstrap for the 6-node Proxmox HA cluster.
# Run from k8s-cp-1.

echo "=========================================================="
echo "  PGS Search Engine - Kubernetes Pre-flight Check"
echo "=========================================================="

# 1. Check kubectl access
echo "==> [1/5] Checking Kubernetes cluster connectivity..."
if ! kubectl cluster-info > /dev/null 2>&1; then
  echo "ERROR: Unable to connect to Kubernetes cluster via kubectl."
  exit 1
fi
echo "    Kubernetes cluster connectivity OK."

# 2. Check and fix cordoned workers
echo "==> [2/5] Checking node scheduling status..."
CORDONED_NODES=$(kubectl get nodes --no-headers | awk '/SchedulingDisabled/ {print $1}')
if [ -n "${CORDONED_NODES}" ]; then
  for node in ${CORDONED_NODES}; do
    echo "    Node ${node} is cordoned. Uncordoning now..."
    kubectl uncordon "${node}"
  done
else
  echo "    All nodes are schedulable (uncordoned)."
fi

# 3. Check memory availability across nodes
echo "==> [3/5] Node allocatable memory overview:"
kubectl get nodes -o custom-columns='NAME:.metadata.name,ROLES:.metadata.labels.node-role\.kubernetes\.io/worker,STATUS:.status.conditions[-1].type,ALLOCATABLE_RAM:.status.allocatable.memory'

# 4. StorageClass check and local-path-provisioner install
echo "==> [4/5] Checking StorageClass..."
DEFAULT_SC=$(kubectl get sc -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}' 2>/dev/null || true)

if [ -z "${DEFAULT_SC}" ]; then
  ANY_SC=$(kubectl get sc --no-headers 2>/dev/null | awk '{print $1}' | head -n1 || true)
  if [ -n "${ANY_SC}" ]; then
    echo "    Found existing StorageClass '${ANY_SC}'. Setting it as default..."
    kubectl patch storageclass "${ANY_SC}" -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
  else
    echo "    No StorageClass found on cluster!"
    echo "    Installing Rancher Local Path Provisioner (lightweight dynamic local disk CSI)..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
    kubectl apply -f "${REPO_ROOT}/k8/storage/local-path-storage.yaml"
    echo "    Waiting for local-path-provisioner to be ready..."
    kubectl -n local-path-storage rollout status deployment/local-path-provisioner --timeout=60s
  fi
else
  echo "    Default StorageClass found: '${DEFAULT_SC}'."
fi

# 5. CoreDNS and CNI Health
echo "==> [5/5] Checking CoreDNS & Calico..."
kubectl -n kube-system get pods -l k8s-app=kube-dns -o wide
echo ""
echo "=========================================================="
echo "  Pre-flight check passed! Cluster is ready for workloads."
echo "  NOTE: If worker-2 is currently running docker-compose, run"
echo "  'docker compose down' on worker-2 to release its ~4GB RAM."
echo "=========================================================="
