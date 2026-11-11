-- Runs automatically the FIRST time the Postgres container starts with an empty volume.
-- Creates a separate database for tutorial data so it never mixes with Airflow's own tables.
-- If you already started Postgres before this file existed, run:
--   docker compose exec postgres psql -U airflow -c "CREATE DATABASE demo;"
CREATE DATABASE demo;
