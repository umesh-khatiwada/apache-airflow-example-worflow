# Optional: extend the official image when you need extra Python packages.
# 1. Add packages to requirements.txt
# 2. In docker-compose.yaml comment out `image:` and uncomment `build: .`
# 3. docker compose build && docker compose up -d
FROM apache/airflow:3.4.0

# Install as the `airflow` user (never as root) and pin to Airflow's constraints
# so pip cannot upgrade/downgrade Airflow itself by accident.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
