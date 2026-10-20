# Cleveland Clinics, OH — ETL + Reporting environment
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/ ./etl/
COPY reports/ ./reports/
COPY data/ ./data/

# Default: run the ETL pipeline against the DB configured by DB_URL.
# Override CMD to run report_generator.py instead — see README.
ENV DB_URL="postgresql+psycopg2://postgres:postgres@db:5432/cleveland_clinics_oh"
CMD ["python", "etl/etl_pipeline.py", "--data-dir", "./data", "--schema-file", "./etl/schema.sql", "--db-url", "${DB_URL}"]
