FROM python:3.12-slim

WORKDIR /opt/dataplatform
COPY pyproject.toml ./
COPY dataplatform ./dataplatform
RUN pip install --no-cache-dir .

CMD ["python", "-m", "dataplatform.bronze.cdc_consumer"]
