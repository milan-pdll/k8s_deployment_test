# PGS Search Engine — Multi-Node Kubernetes Deployment Guide
**Target Cluster:** 6-node HA Kubernetes (Kubespray, v1.37.1, containerd, Calico, IPVS) on Proxmox VMs (10.20.62.0/24 LAN)

---

## 1. Architecture & Cluster Adaptation Summary

Your 6-node cluster differs significantly from a local Docker Desktop setup. Here is how the project has been adapted to run smoothly on your Proxmox environment:

| Challenge in Proxmox Setup | Previous State | Solution Implemented |
|---|---|---|
| **Cordoned Worker** | `k8s-worker-1` was `SchedulingDisabled`. | Automatically uncordoned via `scripts/k8s-cluster-prep.sh`. |
| **Worker RAM Constraints** | VMs have ~4 GB RAM each; `search-engine` requested 4 GiB alone (failing scheduler admission). | Right-sized memory requests (`search-engine` 2Gi, `etl-worker` 1.5Gi) to guarantee scheduler fit across the 3 workers. |
| **containerd Image Isolation** | Built Docker images on `worker-2` aren't visible to `worker-1` or `worker-3`'s `containerd`. | Added `scripts/distribute-images.sh` which directly streams images into all nodes via SSH and `ctr -n k8s.io images import`. |
| **Storage Provisioner** | No dynamic CSI was present; PVCs/StatefulSets would stay `Pending`. | Added `k8/storage/local-path-storage.yaml` (Rancher local-path-provisioner), dynamically provisioning volumes on the 30GB+ VM disks under `/opt/local-path-provisioner`. |
| **Multi-Node Airflow Logs** | Airflow logs PVC is `ReadWriteOnce`. If tasks land on worker-3 while scheduler is on worker-2, pods crash with `Multi-Attach error`. | Added `podAffinity` to `airflow-webserver` and `airflow-pod-template.yaml` ensuring all Airflow pods are co-located on the same node. |
| **Public Exposure & Ingress** | A temporary, manual `cloudflared` process on `cp-1` with random URL. | Added an in-cluster **Nginx Reverse Proxy** (`NodePort: 30080` for college LAN) and an **HA In-Cluster Cloudflare Tunnel** Deployment (`k8/deployments/cloudflared.yaml`). |

---

## 2. Pre-Deployment Step: Release Memory on `k8s-worker-2`

Since `k8s-worker-2` has ~4 GB RAM and currently runs ~12 Docker containers from the old `docker-compose` setup outside Kubernetes, stopping compose frees up memory for Kubernetes workloads:

```bash
# On k8s-worker-2 (10.20.62.105):
cd /home/milan/pgs-search-engine
docker compose down
```

---

## 3. Step-by-Step Deployment Runbook

Run these commands from **`k8s-cp-1`** (or your machine with `kubectl` configured).

### Step 1: Pre-flight Verification & Dynamic Storage Provisioner
Uncordons `k8s-worker-1`, verifies nodes, and installs the Rancher `local-path` StorageClass if not present:

```bash
chmod +x scripts/*.sh
./scripts/k8s-cluster-prep.sh
```

Verify storage:
```bash
kubectl get sc
# Expected: local-path (default)
```

---

### Step 2: Configure Secrets

On the machine where you will run `kubectl`, generate the Kubernetes secrets file once:
```bash
./scripts/prepare-secrets.sh
```
This copies `k8/secrets/secrets.env.example` to `k8/secrets/secrets.env` and generates random values for `API_AUTH_SECRET` and `AIRFLOW_SECRET_KEY`. Keep the generated file; it is gitignored and must exist before applying the manifests.

---

### Step 3: Build & Distribute Container Images to all Nodes
On the node where Docker is installed (e.g. `k8s-worker-2`):

1. **Build the images:**
   ```bash
   docker compose build                                   # Core stack: api, etl, db-migrate, postgres
   docker compose --profile ui build                      # Next.js UI
   docker compose --profile search build                  # gRPC search engine
   ```

2. **Distribute to all 3 worker nodes' `containerd` runtime:**
   Using the automated distributor script (uses passwordless SSH to stream images directly into `containerd`):
   ```bash
   ./scripts/distribute-images.sh import search
   ```
   The `search` distribution includes core images (including UI) and the search engine image.

---

### Step 4: Apply Manifests to the Cluster

Search and the UI/web gateway are enabled by default in `k8/kustomization.yaml`. Once image distribution is complete, this is the only deployment command needed:

```bash
kubectl apply -k k8/
```

Watch the pods spinning up:
```bash
kubectl -n pgs-search-engine get pods -o wide -w
```

---

## 4. How to Access the Cluster

### A. College LAN Access (Zero Internet Dependency)
The Nginx gateway is exposed as a NodePort on port **`30080`**. Open any node IP in your browser:
- **Web UI & API:** `http://10.20.62.101:30080` (or `http://10.20.62.103:30080`, `http://10.20.62.105:30080`)
- **API Docs (FastAPI):** `http://10.20.62.101:30080/api/v1/docs`
- **Health Check:** `http://10.20.62.101:30080/health/ready`

### B. Public Internet Access (HA Cloudflare Tunnel)
The `cloudflared` deployment runs 2 replicas inside Kubernetes.

- **For Quick Tunnel (trycloudflare.com):**
  Retrieve the active public URL anytime:
  ```bash
  kubectl -n pgs-search-engine logs -l app.kubernetes.io/name=cloudflared -c cloudflared | grep -o 'https://.*trycloudflare.com' | head -n 1
  ```

- **For Named Domain Tunnel (e.g. `search.yourdomain.com`):**
  1. Add your tunnel token in `k8/secrets/secrets.env`:
     ```env
     CLOUDFLARE_TUNNEL_TOKEN=eyJhIjoi...
     ```
  2. Switch container args in `k8/deployments/cloudflared.yaml` to `--token $(TUNNEL_TOKEN)`.
  3. Re-apply: `kubectl apply -k k8/`

### C. Developer Port-Forwards
For internal dashboards from your terminal:
```bash
# Airflow UI (login credentials in k8/secrets/secrets.env)
kubectl -n pgs-search-engine port-forward svc/airflow-webserver 8080:8080

# Temporal Workflow UI
kubectl -n pgs-search-engine port-forward svc/temporal-ui 8233:8080

# Direct PostgreSQL Access
kubectl -n pgs-search-engine port-forward svc/postgres 5432:5432
```

---

## 5. Operations & Health Verification

1. **Verify Database Bootstrap:**
   ```bash
   kubectl -n pgs-search-engine get jobs
   kubectl -n pgs-search-engine logs job/db-bootstrap -c migrate
   ```
2. **Verify Spark Cluster:**
   ```bash
   kubectl -n pgs-search-engine logs deploy/spark-master
   kubectl -n pgs-search-engine logs deploy/spark-worker
   ```
3. **Verify OpenSearch:**
   ```bash
   kubectl -n pgs-search-engine exec -it sts/opensearch -- curl http://localhost:9200/_cluster/health?pretty
   ```
4. **Trigger an Airflow DAG:**
   ```bash
   kubectl -n pgs-search-engine exec -it deploy/airflow-scheduler -c scheduler -- airflow dags trigger scraper_crawl_schedule
   ```
5. **Teardown / Clean Slate:**
   ```bash
   kubectl delete -k k8/
   ```
