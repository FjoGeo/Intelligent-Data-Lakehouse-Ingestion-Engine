from datetime import datetime, timezone
from io import BytesIO
import json
import logging
import os
import re

import boto3
import pandas as pd
import requests_cache
from retry_requests import retry
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)
# The API returns hourly forecast values for each of the next seven calendar days.
FORECAST_DAYS = 7


class WeatherIngestion:
    """Fetch and prepare one city's Open-Meteo forecast for lake storage."""

    def __init__(self, city: str, lat: float, lon: float) -> None:
        """Configure a retrying HTTP session and identify the requested location.

        Set OPENMETEO_CACHE_SECONDS to a positive value to cache API responses;
        the default of zero avoids reusing an older forecast between scheduled runs.
        """
        cache_expire_after = int(os.getenv("OPENMETEO_CACHE_SECONDS", "0"))
        self.cache_session = requests_cache.CachedSession(
            ".cache", expire_after=cache_expire_after
        )
        self.retry_session = retry(self.cache_session, retries=5, backoff_factor=0.2)
        self.city = city
        self.lat = lat
        self.lon = lon

    def create_response(self) -> dict:
        """Fetch the raw seven-day hourly forecast as a decoded JSON object."""
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": self.lat,
            "longitude": self.lon,
            "forecast_days": FORECAST_DAYS,
            "timezone": "UTC",
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "rain",
                "snowfall",
                "wind_speed_10m",
                "wind_speed_80m",
                "soil_temperature_0cm",
                "uv_index",
                "is_day",
                "sunshine_duration",
            ],
        }
        response = self.retry_session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_data(self, response: dict, fetched_at: datetime, run_id: str) -> pd.DataFrame:
        """Normalize hourly values and attach metadata used by downstream queries.

        valid_time is when a forecasted value applies; fetched_at is when this
        forecast snapshot was retrieved. Keeping both supports forecast comparisons.
        """
        hourly_data = response["hourly"].copy()
        hourly_data["valid_time"] = pd.to_datetime(hourly_data.pop("time"), utc=True)
        dataframe = pd.DataFrame(hourly_data)
        dataframe["city"] = self.city
        dataframe["latitude"] = self.lat
        dataframe["longitude"] = self.lon
        dataframe["fetched_at"] = fetched_at
        dataframe["ingestion_run_id"] = run_id
        dataframe["source"] = "open-meteo"
        dataframe["dataset_type"] = "forecast"
        dataframe["schema_version"] = 1
        return dataframe

    def store_data(
        self,
        s3,
        bucket_name: str,
        response: dict,
        dataframe: pd.DataFrame,
        run_id: str,
    ) -> tuple[str, str]:
        """Store raw JSON and query-ready Parquet under the same snapshot key.

        Reusing a run_id overwrites that run's two objects, making scheduler retries
        idempotent. A new run_id preserves another forecast snapshot for comparison.
        """
        location = re.sub(r"[^a-z0-9]+", "-", self.city.lower()).strip("-")
        partition = f"location={location}/run_id={run_id}"
        raw_key = f"weather/raw/{partition}/response.json"
        parquet_key = f"weather/forecast/{partition}/data.parquet"

        # Keep the unmodified API payload so the normalized table can be rebuilt later.
        s3.put_object(
            Bucket=bucket_name,
            Key=raw_key,
            Body=json.dumps(response, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
        )

        # Serialize the normalized table in memory; no intermediate local file is needed.
        parquet_buffer = BytesIO()
        dataframe.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        s3.upload_fileobj(
            parquet_buffer,
            bucket_name,
            parquet_key,
            ExtraArgs={"ContentType": "application/vnd.apache.parquet"},
        )
        return raw_key, parquet_key


def create_s3_client():
    """Create the MinIO S3-compatible client using environment overrides.

    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, and MINIO_REGION can
    replace the local Docker Compose defaults.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "adminuser"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "adminpassword"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )


def ensure_bucket_exists(s3, bucket_name: str) -> None:
    """Create the destination bucket if it is missing; surface other S3 errors."""
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        s3.create_bucket(Bucket=bucket_name)


def main() -> None:
    """Ingest forecasts for all configured cities and fail if any city fails.

    Set MINIO_BUCKET to change the destination bucket and INGESTION_RUN_ID to
    supply a scheduler run identifier. Without one, a unique UTC ID is generated.
    """
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    bucket_name = os.getenv("MINIO_BUCKET", "weather-data")
    raw_run_id = os.getenv("INGESTION_RUN_ID")
    run_id = re.sub(
        r"[^A-Za-z0-9_.=-]+",
        "_",
        raw_run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
    )
    locations = [
        ("Hamburg", 53.5507, 9.993),
        ("Moscow", 55.752, 37.6178),
        ("New York", 40.7143, -74.006),
    ]

    s3 = create_s3_client()
    ensure_bucket_exists(s3, bucket_name)
    failures = []

    for city, latitude, longitude in locations:
        try:
            # Record a fetch timestamp separately for each city's API request.
            fetched_at = datetime.now(timezone.utc)
            ingestion = WeatherIngestion(city, latitude, longitude)
            response = ingestion.create_response()
            dataframe = ingestion.get_data(response, fetched_at, run_id)
            raw_key, parquet_key = ingestion.store_data(
                s3, bucket_name, response, dataframe, run_id
            )
            LOGGER.info(
                "Stored %d forecast rows for %s: s3://%s/%s and s3://%s/%s",
                len(dataframe),
                city,
                bucket_name,
                raw_key,
                bucket_name,
                parquet_key,
            )
        except Exception:
            LOGGER.exception("Ingestion failed for %s", city)
            failures.append(city)

    if failures:
        raise RuntimeError(f"Weather ingestion failed for: {', '.join(failures)}")


if __name__ == "__main__":
    main()

    
