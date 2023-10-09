FROM quay.io/astronomer/astro-runtime:9.1.0
# install dbt into a virtual environment
RUN cd dags/dbt/online_retail_transform/ && \
    python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate