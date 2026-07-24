# Intelligent-Data-Lakehouse-Ingestion-Engine

A self-serve tool that connects to multiple data sources (APIs, SQL databases), infers schemas dynamically, normalizes the payload, and streams it into Parquet files in Object Storage (S3/MinIO) or Iceberg tables.

# notice

- MiniIO in docker
- Go script for data ingestion
- Source: CoinGecko / Yahoo Finance or OpenWeather
- second data source required
- Create a Architectural Diagram

## Instructions

start docker

- docker compose up -d

access MiniIO

- http://localhost:9001
