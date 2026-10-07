-- Sets the login password of each service's database role from the environment
-- (run with psql after the migrations: the db-roles service of the root
-- docker-compose.yml, and the db-bootstrap Job in k8/). Baked into the PostgreSQL image
-- (database/Dockerfile) at /opt/pgs/set-role-passwords.sql.
--
-- The roles themselves, their privileges and statement timeouts are created by the
-- Alembic migrations (database/src/pgs_db/grants.py) LOGIN without a password; per
-- database/README.md §7 an operator sets one per environment. Passwords are read with
-- \getenv, so they never appear on a command line, and quoted by psql's :'var'.
-- Re-running is harmless: it just sets the same passwords again.

\set ON_ERROR_STOP on

\getenv api_password PGS_API_DB_PASSWORD
\getenv etl_password PGS_ETL_DB_PASSWORD
\getenv search_password PGS_SEARCH_DB_PASSWORD
\getenv scraper_password PGS_SCRAPER_DB_PASSWORD
\getenv jobs_password PGS_JOBS_DB_PASSWORD

ALTER ROLE pgs_api PASSWORD :'api_password';
ALTER ROLE pgs_etl PASSWORD :'etl_password';
ALTER ROLE pgs_search PASSWORD :'search_password';
ALTER ROLE pgs_scraper PASSWORD :'scraper_password';
-- python -m pgs_db.jobs (Gold summaries, stale claims, retention), run by Airflow.
ALTER ROLE pgs_jobs PASSWORD :'jobs_password';
