FROM apache/airflow:2.9.1-python3.10

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create the dbt directory structure
RUN mkdir -p /opt/airflow/dbt && \
    chown -R airflow:root /opt/airflow/dbt && \
    chmod -R 775 /opt/airflow/dbt

# Install dbt into the system Python environment
RUN python -m pip install --no-cache-dir \
    dbt-core==1.7.3 \
    dbt-snowflake==1.7.1 \
    apache-airflow-providers-snowflake \
    snowflake-connector-python \
    retail-data-sources==1.0.9

# Verify installation of packages
RUN python -m pip list

# Make sure dbt is executable
RUN which dbt && chmod 755 $(which dbt)

# Create .dbt directory for profiles
RUN mkdir -p /home/airflow/.dbt && \
    chown -R airflow:root /home/airflow/.dbt && \
    chmod -R 775 /home/airflow/.dbt

# Switch to airflow user
USER airflow
WORKDIR /opt/airflow

# Verify dbt installation
RUN dbt --version