# Backend Ingestion Script

This backend folder contains a Go script that fetches financial market data from two sources and uploads it to MinIO as CSV objects.

## Data sources

- Yahoo Finance (`yahoo-%s.csv`)
- AlphaVantage (`alphavantage-%s.csv`)

## Requirements

- Go 1.22+
- Docker and the MinIO container from `infrastructure/docker-compose.yml`
- An internet connection to access the finance APIs

## Setup

1. Start MinIO:
   ```bash
   cd ../infrastructure
   docker compose up -d
   ```

2. Verify MinIO is running at `http://localhost:9001`.

3. Build or run the ingestion script:
   ```bash
   cd ../backend
   go run main.go
   ```

## Configuration

Set environment variables to override defaults:

- `MINIO_ENDPOINT` (default: `localhost:9000`)
- `MINIO_ACCESS_KEY_ID` (default: `adminuser`)
- `MINIO_SECRET_ACCESS_KEY` (default: `adminpassword`)
- `MINIO_BUCKET` (default: `finance-data`)
- `MINIO_USE_SSL` (default: `false`)
- `YAHOO_SYMBOL` (default: `AAPL`)
- `ALPHAVANTAGE_SYMBOL` (default: `MSFT`)
- `ALPHAVANTAGE_API_KEY` (default: `demo`)

Example:

```bash
MINIO_ENDPOINT=localhost:9000 \
  MINIO_ACCESS_KEY_ID=adminuser \
  MINIO_SECRET_ACCESS_KEY=adminpassword \
  ALPHAVANTAGE_API_KEY=demo \
  go run main.go
```

## Notes

- Replace `ALPHAVANTAGE_API_KEY` with your own API key for reliable usage.
- The script writes CSV files to a temporary folder and then uploads them to the configured MinIO bucket.
