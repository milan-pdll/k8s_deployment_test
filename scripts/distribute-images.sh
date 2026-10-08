#!/usr/bin/env bash
set -euo pipefail

# scripts/distribute-images.sh
# Builds and distributes container images across the 6-node Kubernetes cluster.
#
# Containerd does not automatically share Docker daemon images across VMs.
# This script offers two modes:
#   1) "import" (Default): Direct SSH stream of `docker save` into `sudo ctr -n k8s.io images import -`
#      Zero external network usage, works directly across the local Proxmox LAN!
#   2) "registry": Tag and push to an in-cluster or LAN registry (e.g., 10.20.62.101:5000).

WORKER_NODES=("10.20.62.103" "10.20.62.105" "10.20.62.107")
ALL_NODES=("10.20.62.101" "10.20.62.104" "10.20.62.106" "10.20.62.103" "10.20.62.105" "10.20.62.107")
SSH_USER="${SSH_USER:-milan}"

CORE_IMAGES=(
  "pgs-search-engine/postgres:local"
  "pgs-search-engine/db-migrate:local"
  "pgs-search-engine/api:local"
  "pgs-search-engine/etl:local"
  "pgs-search-engine/ui:local"
)

OPTIONAL_SEARCH_IMAGES=(
  "pgs-search-engine/search-engine:local"
)

OPTIONAL_SCRAPER_IMAGES=(
  "pgs-search-engine/scraper-worker:local"
  "pgs-search-engine/scraper-api:local"
  "pgs-search-engine/scraper-cli:local"
)

echo "=========================================================="
echo "  PGS Search Engine - Multi-Node Image Distributor"
echo "=========================================================="

MODE="${1:-import}"
PROFILE="${2:-core}" # core, search, all

IMAGES=("${CORE_IMAGES[@]}")
if [ "${PROFILE}" == "search" ] || [ "${PROFILE}" == "all" ]; then
  IMAGES+=("${OPTIONAL_SEARCH_IMAGES[@]}")
fi
if [ "${PROFILE}" == "scraper" ] || [ "${PROFILE}" == "all" ]; then
  IMAGES+=("${OPTIONAL_SCRAPER_IMAGES[@]}")
fi

echo "Images to distribute (${#IMAGES[@]}):"
for img in "${IMAGES[@]}"; do
  echo "  - ${img}"
done
echo ""

if [ "${MODE}" == "import" ]; then
  echo "==> Mode: Direct SSH containerd stream (ctr -n k8s.io images import)"
  echo "Target nodes: ${WORKER_NODES[*]}"
  echo ""

  for img in "${IMAGES[@]}"; do
    if ! docker image inspect "${img}" > /dev/null 2>&1; then
      echo "WARNING: Docker image '${img}' not found locally. Skipping."
      continue
    fi

    echo "--> Packaging and distributing image: ${img}"
    for node_ip in "${WORKER_NODES[@]}"; do
      echo "    -> Importing to node ${node_ip}..."
      if ip addr | grep -q "${node_ip}"; then
        # Local node: import directly without SSH
        docker save "${img}" | ctr -n k8s.io images import -
      else
        docker save "${img}" | ssh -o StrictHostKeyChecking=no "${SSH_USER}@${node_ip}" "sudo ctr -n k8s.io images import -"
      fi
    done
  done

  echo ""
  echo "==> Image distribution complete! All worker nodes have images in containerd."

elif [ "${MODE}" == "registry" ]; then
  REGISTRY="${REGISTRY:-10.20.62.101:5000}"
  echo "==> Mode: Registry push to ${REGISTRY}"
  for img in "${IMAGES[@]}"; do
    TARGET_TAG="${REGISTRY}/${img}"
    echo "--> Tagging ${img} as ${TARGET_TAG}"
    docker tag "${img}" "${TARGET_TAG}"
    docker push "${TARGET_TAG}"
  done
  echo "==> Registry push complete."
else
  echo "Usage: $0 [import|registry] [core|search|all]"
  exit 1
fi
