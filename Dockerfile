FROM quay.io/astronomer/astro-runtime:9.1.0

# Install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

# Airflow
#ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true

# Alias
RUN echo "alias cl='clear'" >> ~/.bashrc && source ~/.bashrc
RUN echo "alias dbt-act='source /usr/local/airflow/dbt_venv/bin/activate'" >> ~/.bashrc && source ~/.bashrc
