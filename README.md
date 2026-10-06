# Intelligent-Data-Lakehouse-Ingestion-Engine

A self-serve tool that connects to multiple data sources (APIs, SQL databases),
infers schemas dynamically, normalizes the payload, and streams it into Parquet files in Object Storage (S3/MinIO) or Iceberg tables.

# to do

- learn S3, MinIO, parquet
- Python for air quality
- ingestion into MinIO
- Python --> docker --> Airflow

# notice

- MiniIO in docker
- Python script to get air quality and weather data --> OpenMeteo + OpenAQ
- Store in PostGreSQL
- Create a Architectural Diagram

## Instructions

start docker

- docker compose up -d

access MiniIO

- http://localhost:9001
