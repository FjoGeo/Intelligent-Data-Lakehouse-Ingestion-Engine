from datetime import datetime, timezone
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


class AirqualityIngestion:
    """Fetch and prepare one city's OpenAQ air quality data for lake storage."""

    def __init__(self, 
        locations: list, 
        logger: logging.Logger, 
        open_api_url: str, 
        page_size: int,
        measurement_columns: list) -> None:
        pass


if __name__ == "__main__":
    load_dotenv()
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)
    locations = [
        ("Hamburg", 53.5507, 9.993),
        ("Moscow", 55.752, 37.6178),
        ("New York", 40.7143, -74.006),
    ]
    open_api_url = "https://api.openaq.org/v3"
    page_size = 100
    measurement_columns = [
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