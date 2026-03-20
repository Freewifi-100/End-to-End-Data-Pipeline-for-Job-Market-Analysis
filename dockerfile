FROM apache/airflow:3.1.8

USER root

# Optional system utilities used by some scripts and diagnostics.
RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt and Databricks-related Python dependencies directly in image
# so Airflow tasks can run dbt reliably without startup-time package installs.
RUN pip install --no-cache-dir \
	dbt-core==1.11.5 \
	dbt-databricks==1.10.9 \
	databricks-sdk \
	databricks-sql-connector \
	python-dotenv
