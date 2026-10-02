FROM apache/airflow:3.3.2

# Pipeline engine dependencies (the platform code itself is mounted at /opt/project).
RUN pip install --no-cache-dir "deltalake==1.6.6" "duckdb==1.5.6" "psycopg[binary]>=3.2" \
    "openpyxl>=3.1" "obstore>=0.8" certifi
