FROM quay.io/astronomer/astro-runtime:11.7.0

RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install --no-cache-dir soda-core-bigquery==3.0.45 &&\
    pip install --no-cache-dir soda-core-scientific==3.0.45 && deactivate

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery==1.5.3 && deactivate

RUN mkdir -p /tmp/dbt_logs /tmp/dbt_packages

# (Optional) Set GCS bucket environment variable
ENV GCP_GCS_BUCKET=nyc_project_sigma_heuristic
