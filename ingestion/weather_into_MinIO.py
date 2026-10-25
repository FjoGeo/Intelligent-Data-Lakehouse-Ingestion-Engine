from datetime import date, datetime, timedelta, timezone
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
# The archive request covers seven complete UTC calendar days.
HISTORY_DAYS = 7


class WeatherIngestion:
    """Fetch and prepare one city's historical Open-Meteo data for lake storage."""

    def __init__(
        self, city: str, lat: float, lon: float, start_date: date, end_date: date
    ) -> None:
        """Configure a retrying HTTP session and identify the requested location.

        Set OPENMETEO_CACHE_SECONDS to a positive value to cache API responses;
        the default of zero avoids reusing a cached response between scheduled runs.
        """
        cache_expire_after = int(os.getenv("OPENMETEO_CACHE_SECONDS", "0"))
        self.cache_session = requests_cache.CachedSession(
            ".cache", expire_after=cache_expire_after
        )
        self.retry_session = retry(self.cache_session, retries=5, backoff_factor=0.2)
        self.city = city
        self.lat = lat
        self.lon = lon
        self.start_date = start_date
        self.end_date = end_date

    def create_response(self) -> dict:
        """Fetch hourly historical data for the requested inclusive date range."""
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": self.lat,
            "longitude": self.lon,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "timezone": "UTC",
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "rain",
                "snowfall",
                "wind_speed_10m",
                "wind_speed_100m",
                "soil_temperature_0_to_7cm",
                "weather_code",
                "sunshine_duration",
            ],
        }
        response = self.retry_session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_data(self, response: dict, fetched_at: datetime, run_id: str) -> pd.DataFrame:
        """Normalize hourly values and attach metadata used by downstream queries.

        valid_time is when each historical value applies; fetched_at records when
        this archive response was retrieved.
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
        dataframe["dataset_type"] = "historical_weather"
        dataframe["data_start_date"] = self.start_date.isoformat()
        dataframe["data_end_date"] = self.end_date.isoformat()
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
        """Store raw JSON and query-ready Parquet under the same run key.

        Reusing a run_id overwrites that run's two objects, making scheduler retries
        idempotent. A new run_id preserves another historical ingestion snapshot.
        """
        location = re.sub(r"[^a-z0-9]+", "-", self.city.lower()).strip("-")
        partition = f"location={location}/run_id={run_id}"
        raw_key = f"weather/raw/{partition}/response.json"
        parquet_key = f"weather/historical/{partition}/data.parquet"

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
    """Ingest the previous seven complete UTC days for all configured cities.

    Set MINIO_BUCKET to change the destination bucket and INGESTION_RUN_ID to
    supply a scheduler run identifier. The default ID is based on the date window,
    so rerunning the same weekly window overwrites its objects.
    """
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    bucket_name = os.getenv("MINIO_BUCKET", "weather-data")
    today_utc = datetime.now(timezone.utc).date()
    start_date = today_utc - timedelta(days=HISTORY_DAYS)
    end_date = today_utc - timedelta(days=1)
    raw_run_id = os.getenv("INGESTION_RUN_ID")
    run_id = re.sub(
        r"[^A-Za-z0-9_.=-]+",
        "_",
        raw_run_id or f"{start_date:%Y%m%d}_{end_date:%Y%m%d}",
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
            ingestion = WeatherIngestion(
                city, latitude, longitude, start_date, end_date
            )
            response = ingestion.create_response()
            dataframe = ingestion.get_data(response, fetched_at, run_id)
            raw_key, parquet_key = ingestion.store_data(
                s3, bucket_name, response, dataframe, run_id
            )
            LOGGER.info(
                "Stored %d historical weather rows for %s: s3://%s/%s and s3://%s/%s",
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

    
