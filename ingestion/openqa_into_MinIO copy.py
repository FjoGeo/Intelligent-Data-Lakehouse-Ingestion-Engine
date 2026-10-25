from datetime import datetime, time, timedelta, timezone
from io import BytesIO
import json
import logging
import os
import re

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)
OPENAQ_API_URL = "https://api.openaq.org/v3"
PAGE_SIZE = 100
MEASUREMENT_COLUMNS = [
	"city",
	"location_id",
	"location_name",
	"sensor_id",
	"parameter_id",
	"parameter_name",
	"parameter_display_name",
	"parameter_units",
	"value",
	"measured_at",
	"latitude",
	"longitude",
	"fetched_at",
	"ingestion_run_id",
	"source",
	"dataset_type",
	"data_start_time",
	"data_end_time",
	"schema_version",
]
LOCATIONS = [
	("Hamburg", 53.5507, 9.993),
	("Moscow", 55.752, 37.6178),
	("New York", 40.7143, -74.006),
]


def create_api_session(api_key: str) -> requests.Session:
	"""Create an OpenAQ HTTP session with authentication and transient retries."""
	session = requests.Session()
	session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})
	retry_policy = Retry(
		total=5,
		connect=5,
		read=5,
		status=5,
		backoff_factor=0.5,
		status_forcelist=(429, 500, 502, 503, 504),
		allowed_methods=frozenset({"GET"}),
		respect_retry_after_header=True,
	)
	session.mount("https://", HTTPAdapter(max_retries=retry_policy))
	return session


def get_json(session: requests.Session, path: str, params: dict | None = None) -> dict:
	"""Request one OpenAQ endpoint and return its JSON response."""
	response = session.get(f"{OPENAQ_API_URL}{path}", params=params, timeout=30)
	response.raise_for_status()
	return response.json()


def get_city_locations(
	session: requests.Session,
	latitude: float,
	longitude: float,
	radius_meters: int,
) -> list[dict]:
	"""List all OpenAQ monitoring locations within the requested city radius."""
	locations = []
	page = 1
	while True:
		payload = get_json(
			session,
			"/locations",
			{
				"coordinates": f"{latitude},{longitude}",
				"radius": radius_meters,
				"limit": PAGE_SIZE,
				"page": page,
			},
		)
		page_results = payload.get("results", [])
		locations.extend(page_results)
		if len(page_results) < PAGE_SIZE:
			return locations
		page += 1


def get_location_sensors(session: requests.Session, location_id: int) -> list[dict]:
	"""List every sensor registered at a location, following all result pages."""
	sensors = []
	page = 1
	while True:
		payload = get_json(
			session,
			f"/locations/{location_id}/sensors",
			{"limit": PAGE_SIZE, "page": page},
		)
		page_results = payload.get("results", [])
		sensors.extend(page_results)
		if len(page_results) < PAGE_SIZE:
			return sensors
		page += 1


def get_datetime_value(value: dict | str | None) -> str | None:
	"""Extract a measurement timestamp, preferring its UTC representation."""
	if isinstance(value, dict):
		return value.get("utc") or value.get("local")
	return value


def collect_city_data(
	session: requests.Session,
	city: str,
	latitude: float,
	longitude: float,
	radius_meters: int,
	start_time: datetime,
	end_time: datetime,
	fetched_at: datetime,
	run_id: str,
) -> tuple[dict, pd.DataFrame]:
	"""Fetch all sensor measurements in the requested time range near a city."""
	locations = get_city_locations(session, latitude, longitude, radius_meters)
	raw_locations = []
	rows = []

	for location in locations:
		location_id = location.get("id")
		if location_id is None:
			LOGGER.warning("Skipping a %s location without an ID", city)
			continue

		coordinates = location.get("coordinates") or {}
		sensors = get_location_sensors(session, location_id)
		raw_sensors = []
		for sensor in sensors:
			sensor_id = sensor.get("id")
			if sensor_id is None:
				LOGGER.warning("Skipping a sensor without an ID at location %s", location_id)
				continue

			measurement_pages = []
			page = 1
			while True:
				measurement_payload = get_json(
					session,
					f"/sensors/{sensor_id}/measurements",
					{
						"datetime_from": start_time.isoformat().replace("+00:00", "Z"),
						"datetime_to": end_time.isoformat().replace("+00:00", "Z"),
						"limit": PAGE_SIZE,
						"page": page,
					},
				)
				measurement_pages.append(measurement_payload)
				measurements = measurement_payload.get("results", [])
				parameter = sensor.get("parameter") or {}
				for measurement in measurements:
					rows.append(
						{
							"city": city,
							"location_id": location_id,
							"location_name": location.get("name"),
							"sensor_id": sensor_id,
							"parameter_id": parameter.get("id"),
							"parameter_name": parameter.get("name"),
							"parameter_display_name": parameter.get("displayName"),
							"parameter_units": parameter.get("units"),
							"value": measurement.get("value"),
							"measured_at": get_datetime_value(measurement.get("datetime")),
							"latitude": coordinates.get("latitude"),
							"longitude": coordinates.get("longitude"),
							"fetched_at": fetched_at,
							"ingestion_run_id": run_id,
							"source": "openaq",
							"dataset_type": "historical_measurement",
							"data_start_time": start_time.isoformat(),
							"data_end_time": end_time.isoformat(),
							"schema_version": 1,
						}
					)
				if len(measurements) < PAGE_SIZE:
					break
				page += 1
			raw_sensors.append({"sensor": sensor, "measurements": measurement_pages})

		raw_locations.append({"location": location, "sensors": raw_sensors})

	raw_payload = {
		"city": city,
		"center": {"latitude": latitude, "longitude": longitude},
		"radius_meters": radius_meters,
		"data_start_time": start_time.isoformat(),
		"data_end_time": end_time.isoformat(),
		"fetched_at": fetched_at.isoformat(),
		"ingestion_run_id": run_id,
		"locations": raw_locations,
	}
	return raw_payload, pd.DataFrame(rows, columns=MEASUREMENT_COLUMNS)


def create_s3_client():
	"""Create a MinIO-compatible S3 client from environment configuration."""
	return boto3.client(
		"s3",
		endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
		aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "adminuser"),
		aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "adminpassword"),
		region_name=os.getenv("MINIO_REGION", "us-east-1"),
	)


def ensure_bucket_exists(s3, bucket_name: str) -> None:
	"""Create the destination bucket if it does not already exist."""
	try:
		s3.head_bucket(Bucket=bucket_name)
	except ClientError as error:
		code = error.response.get("Error", {}).get("Code")
		if code not in {"404", "NoSuchBucket", "NotFound"}:
			raise
		s3.create_bucket(Bucket=bucket_name)


def store_city_data(
	s3,
	bucket_name: str,
	city: str,
	raw_payload: dict,
	dataframe: pd.DataFrame,
	run_id: str,
) -> tuple[str, str]:
	"""Write the complete source response and flattened readings to MinIO."""
	location = re.sub(r"[^a-z0-9]+", "-", city.lower()).strip("-")
	prefix = f"air-quality/location={location}/run_id={run_id}"
	raw_key = f"{prefix}/raw.json"
	parquet_key = f"{prefix}/measurements.parquet"

	s3.put_object(
		Bucket=bucket_name,
		Key=raw_key,
		Body=json.dumps(raw_payload, separators=(",", ":")).encode("utf-8"),
		ContentType="application/json",
	)

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


def main() -> None:
	"""Ingest the previous seven complete UTC days near the configured cities.

	The search radius defaults to 25 km and can be changed with
	OPENAQ_RADIUS_METERS. Set INGESTION_RUN_ID from a scheduler for idempotent retries.
	"""
	load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
	logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())

	api_key = os.getenv("openaq_key") or os.getenv("OPENAQ_API_KEY")
	if not api_key:
		raise RuntimeError("Set openaq_key in ingestion/.env or OPENAQ_API_KEY in the environment")

	radius_meters = int(os.getenv("OPENAQ_RADIUS_METERS", "25000"))
	bucket_name = os.getenv("MINIO_BUCKET", "air-quality-data")
	end_time = datetime.combine(datetime.now(timezone.utc).date(), time.min, tzinfo=timezone.utc)
	start_time = end_time - timedelta(days=7)
	run_id = re.sub(
		r"[^A-Za-z0-9_.=-]+",
		"_",
		os.getenv("INGESTION_RUN_ID")
		or f"{start_time:%Y%m%d}_{(end_time - timedelta(days=1)):%Y%m%d}",
	)

	session = create_api_session(api_key)
	s3 = create_s3_client()
	ensure_bucket_exists(s3, bucket_name)
	failures = []

	for city, latitude, longitude in LOCATIONS:
		try:
			fetched_at = datetime.now(timezone.utc)
			raw_payload, dataframe = collect_city_data(
				session,
				city,
				latitude,
				longitude,
				radius_meters,
				start_time,
				end_time,
				fetched_at,
				run_id,
			)
			raw_key, parquet_key = store_city_data(
				s3, bucket_name, city, raw_payload, dataframe, run_id
			)
			LOGGER.info(
				"Stored %d historical readings from %d locations for %s at s3://%s/%s and s3://%s/%s",
				len(dataframe),
				len(raw_payload["locations"]),
				city,
				bucket_name,
				raw_key,
				bucket_name,
				parquet_key,
			)
		except Exception:
			LOGGER.exception("OpenAQ ingestion failed for %s", city)
			failures.append(city)

	if failures:
		raise RuntimeError(f"OpenAQ ingestion failed for: {', '.join(failures)}")


if __name__ == "__main__":
	main()
