# Kubernetes

Kubernetes manifests for the whole project, written with Kustomize (built into `kubectl`).
This is a **single-node, non-HA** setup: one replica of everything and ReadWriteOnce
volumes, aimed at Docker Desktop's Kubernetes. The default manifest includes the search
engine and UI/web gateway, alongside the services in the root `docker-compose.yml`. One
difference: **Airflow uses the KubernetesExecutor, so every DAG task runs in its own pod**.

## Prerequisites

- Docker Desktop with Kubernetes enabled (*Settings → Kubernetes → Enable Kubernetes*;
  with the kubeadm option, Kubernetes sees the images in the local Docker engine) and
  `kubectl` pointing at it: `kubectl config use-context docker-desktop`.
- The project images, built with Compose from the repository root:
  ```bash
  docker compose build                                  # core images
  docker compose --profile ui --profile search build    # UI and search images enabled by default in Kustomize
  ```
  They are tagged `pgs-search-engine/<name>:local` and used as is (`imagePullPolicy:
  IfNotPresent`). To use a registry instead, push them and set `images:` in
  `kustomization.yaml` and the task-pod image in `configmaps/airflow-pod-template.yaml`.
- Memory: the core stack requests ~4.6 GiB; search adds about 5 GiB and downloads ~4.5 GB
  of models on first start. Allow up to 3 GiB per running Airflow task (LaBSE embeddings).
  Docker Desktop on WSL gets half the host RAM by default; raise it in
  `%UserProfile%\.wslconfig` (`[wsl2]` `memory=12GB`) before deploying the full default stack.

## Deploy

```bash
./scripts/prepare-secrets.sh                              # once; generates required secrets
kubectl apply -k k8/
kubectl -n pgs-search-engine get pods -w
```

Start-up order is enforced with init containers instead of compose's `depends_on`: the
`db-bootstrap` Job migrates (Alembic), seeds and sets the service roles' passwords; the API and
search engine wait until they can log in as their role and see the seeded
data. Airflow's scheduler and webserver wait for `airflow-init` to migrate Airflow's own
database. The bootstrap Jobs delete themselves 10 minutes after finishing, so re-running
`kubectl apply -k k8/` (e.g. after rebuilding images with a new migration) runs them again.

Remove everything (volumes included): `kubectl delete -k k8/`.

## Airflow: DAG tasks as pods

| Object | What it does |
| --- | --- |
| `airflow-scheduler` | Parses the DAGs and launches **one pod per task instance** from the pod template (`configmaps/airflow-pod-template.yaml`), through the Kubernetes API (`roles/airflow-pod-launcher.yaml`). It runs no tasks itself. |
| task pods (`app.kubernetes.io/name=airflow-task`) | The ETL image running `airflow tasks run ...`; they reach Kafka, ClamAV, OpenSearch and PostgreSQL by Service name. Deleted when the task succeeds, kept when it fails. |
| `airflow-webserver` | UI; reads task logs from the shared `airflow-logs` volume. |
| `airflow-db` | Airflow's metadata database. |

There is no Celery worker or Redis here. At most 4 task pods run at once
(`AIRFLOW__CORE__PARALLELISM`). DAGs are baked into the ETL image: after changing a DAG,
`docker compose build airflow-scheduler` and
`kubectl -n pgs-search-engine rollout restart deploy/airflow-scheduler deploy/airflow-webserver`.

Run the ETL pipeline: with the `scraper` block enabled, each website whose crawl finishes
publishes one event to `scraped_files_topic`; `etl_ingestion_pipeline` batches the events and
starts one `EtlBatchWorkflow` on Temporal per batch. The `etl-worker` Deployment runs it as
the Spark driver on `spark-master` / `spark-worker` (scan, extraction, LaBSE) and saves the
pages to PostgreSQL; with the `search` block, `search-indexer` copies them into OpenSearch.

```bash
kubectl -n pgs-search-engine port-forward svc/airflow-webserver 8080:8080
# http://localhost:8080 (login from secrets/secrets.env): etl_ingestion_pipeline runs
kubectl -n pgs-search-engine get pods -l app.kubernetes.io/name=airflow-task -w
kubectl -n pgs-search-engine logs deploy/etl-worker -f                 # one summary per site
```

## Kustomize components

Search and the UI/web gateway are enabled by default. The remaining optional parts are
commented-out blocks at the end of `resources:` in `kustomization.yaml`: uncomment a whole
block, then `kubectl apply -k k8/`.

| Component | Adds |
| --- | --- |
| `search` (default) | gRPC search engine (`search-engine:50051`, gRPC health probes) and the search indexer |
| `scraper` | LocalStack S3 (+ browser), headless Chrome (with a NetworkPolicy keeping it off the cluster network), crawler worker, documents API |
| `scraper-sharded` | with `scraper`: three host-sharded workers (StatefulSet) instead of the worker Deployment; also uncomment the `patches:` block |
| `ui` (default) | Next.js UI, Nginx gateway, Cloudflare tunnel |
| `tools` | OpenSearch Dashboards |

Temporal (with its own PostgreSQL and UI) and the Spark cluster are part of the default stack:
both the crawl and the ETL run on them. Airflow's `scraper_crawl_schedule` DAG starts crawls;
a one-off crawl is a Job in `jobs/on-demand/` (`kubectl create -f`):
`SEEDS=https://example.gov.np envsubst < k8/jobs/on-demand/scraper-crawl.yaml.tmpl | kubectl create -f -`
(or `make k8s-crawl SEEDS=...` in `scraper/`).

## Access from the host

Every Service is ClusterIP (nothing is exposed outside the cluster); use port-forwards,
which bind to localhost:

| Service | Command |
| --- | --- |
| API | `kubectl -n pgs-search-engine port-forward svc/api 8000:8000` |
| Airflow | `kubectl -n pgs-search-engine port-forward svc/airflow-webserver 8080:8080` |
| PostgreSQL | `kubectl -n pgs-search-engine port-forward svc/postgres 5432:5432` |
| OpenSearch | `kubectl -n pgs-search-engine port-forward svc/opensearch 9200:9200` |
| Kafka (host scripts) | `kubectl -n pgs-search-engine port-forward svc/kafka 9092:9092` |
| UI | `kubectl -n pgs-search-engine port-forward svc/ui 3000:3000` |
| Temporal UI | `kubectl -n pgs-search-engine port-forward svc/temporal-ui 8233:8080` |
| Scraper API | `kubectl -n pgs-search-engine port-forward svc/scraper-api 8082:8080` (`/docs`) |
| S3 browser | `kubectl -n pgs-search-engine port-forward svc/s3-browser 8081:8080` |
| OpenSearch Dashboards | `kubectl -n pgs-search-engine port-forward svc/opensearch-dashboards 5601:5601` |

## Layout

Grouped by resource kind, one resource per file named after it (`services/api.yaml` is the
`api` Service, `deployments/api.yaml` its Deployment). `kustomization.yaml` lists every
file; a new resource goes into its kind's folder and into that list.

```text
k8/
├── kustomization.yaml        what gets applied (default stack + optional blocks), secrets, image tags
├── namespaces/               pgs-search-engine (Pod Security: enforce baseline, warn restricted)
├── configmaps/               pgs-config (service addresses, embedding model), airflow-config, airflow-pod-template
│                             (the KubernetesExecutor's task pod), scraper-config, s3-init
├── secrets/                  secrets.env.example -> secrets.env (gitignored) -> Secret pgs-secrets
├── persistentvolumeclaims/   Airflow logs, ClamAV signatures, model caches
├── serviceaccounts/          Airflow scheduler, webserver, task pods
├── roles/                    pod launcher (scheduler), pod log reader (webserver)
├── rolebindings/             binds those roles to the service accounts
├── services/                 one ClusterIP Service per workload
├── statefulsets/             PostgreSQL, Kafka, OpenSearch, Airflow DB, Temporal DB, sharded crawler
├── deployments/              API, ClamAV, Airflow scheduler/webserver, Temporal, Spark master/
│                             workers, ETL worker, search engine, search indexer, scraper, S3,
│                             Chrome, UI, Dashboards
├── jobs/                     bootstrap Jobs applied with the stack (db-bootstrap, kafka-init, airflow-init)
│   └── on-demand/            one-off Jobs, created with `kubectl create -f`
└── networkpolicies/          chrome-egress (Chrome: internet + DNS only)
```

## Not covered (non-HA by design)

- No replication or failover: one PostgreSQL, Kafka, OpenSearch and Temporal each.
- Shared volumes (`airflow-logs`, `etl-models`, `search-models`) rely on all pods being on
  one node; on a multi-node cluster use a ReadWriteMany storage class or remote task logs.
- No Ingress, autoscaling or backups; the only NetworkPolicy is Chrome's (it needs a CNI that
  enforces them). The PostgreSQL schema stays owned by
  the `pgs-db` migrations; the scraper has no migrations of its own.
