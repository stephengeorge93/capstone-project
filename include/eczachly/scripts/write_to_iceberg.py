import os
import requests
import time
import logging
import hashlib
from datetime import date
from dotenv import load_dotenv

import pyarrow as pa
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, DoubleType, BooleanType, LongType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.catalog import load_catalog
from eczachly.aws_secret_manager import get_secret

# Load env vars and setup logging
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# RentCast API config
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.rentcast.io/v1/properties"
LIMIT = 500
MAX_PAGES = 10
HEADERS = {"Accept": "application/json", "X-Api-Key": API_KEY}

# Iceberg config
TABLE_NAME = "stephengeorge93.rentcast_properties"

# Hashing helper
def hash_property_id(value):
    return hashlib.sha256(value.encode()).hexdigest() if value else None

# Flatten a RentCast record
def flatten_property(p):
    return {
        "property_id": hash_property_id(p.get("id") or "UNKNOWN"),
        "formatted_address": p.get("formattedAddress"),
        "address_line_1": p.get("addressLine1"),
        "city": p.get("city"),
        "state": p.get("state"),
        "zip_code": p.get("zipCode"),
        "county": p.get("county"),
        "latitude": p.get("latitude"),
        "longitude": p.get("longitude"),
        "property_type": p.get("propertyType"),
        "bedrooms": p.get("bedrooms"),
        "bathrooms": p.get("bathrooms"),
        "square_footage": p.get("squareFootage"),
        "lot_size": p.get("lotSize"),
        "year_built": p.get("yearBuilt"),
        "subdivision": p.get("subdivision"),
        "last_sale_date": p.get("lastSaleDate"),
        "floor_count": p.get("features", {}).get("floorCount"),
        "garage": p.get("features", {}).get("garage"),
        "garage_spaces": p.get("features", {}).get("garageSpaces"),
        "garage_type": p.get("features", {}).get("garageType"),
        "heating_type": p.get("features", {}).get("heatingType"),
        "cooling_type": p.get("features", {}).get("coolingType"),
        "roof_type": p.get("features", {}).get("roofType"),
        "unit_count": p.get("features", {}).get("unitCount"),
        "room_count": p.get("features", {}).get("roomCount"),
        "architecture_type": p.get("features", {}).get("architectureType"),
        "value_2023": p.get("taxAssessments", {}).get("2023", {}).get("value"),
        "land_2023": p.get("taxAssessments", {}).get("2023", {}).get("land"),
        "improvements_2023": p.get("taxAssessments", {}).get("2023", {}).get("improvements"),
        "value_2024": p.get("taxAssessments", {}).get("2024", {}).get("value"),
        "land_2024": p.get("taxAssessments", {}).get("2024", {}).get("land"),
        "improvements_2024": p.get("taxAssessments", {}).get("2024", {}).get("improvements"),
        "tax_2023": p.get("propertyTaxes", {}).get("2023", {}).get("total"),
        "owner_type": p.get("owner", {}).get("type"),
        "owner_names": ", ".join(p.get("owner", {}).get("names", [])),
        "owner_zip": p.get("owner", {}).get("mailingAddress", {}).get("zipCode"),
        "owner_city": p.get("owner", {}).get("mailingAddress", {}).get("city"),
        "owner_state": p.get("owner", {}).get("mailingAddress", {}).get("state"),
        "load_date": date.today().isoformat()
    }

# Iceberg schema + partition
schema = Schema(
    NestedField(1, "property_id", StringType(), False),
    NestedField(2, "formatted_address", StringType(), False),
    NestedField(3, "address_line_1", StringType(), False),
    NestedField(4, "city", StringType(), False),
    NestedField(5, "state", StringType(), False),
    NestedField(6, "zip_code", StringType(), False),
    NestedField(7, "county", StringType(), False),
    NestedField(8, "latitude", DoubleType(), False),
    NestedField(9, "longitude", DoubleType(), False),
    NestedField(10, "property_type", StringType(), False),
    NestedField(11, "bedrooms", LongType(), False),
    NestedField(12, "bathrooms", DoubleType(), False),
    NestedField(13, "square_footage", LongType(), False),
    NestedField(14, "lot_size", LongType(), False),
    NestedField(15, "year_built", LongType(), False),
    NestedField(16, "subdivision", StringType(), False),
    NestedField(17, "last_sale_date", StringType(), False),
    NestedField(18, "floor_count", LongType(), False),
    NestedField(19, "garage", BooleanType(), False),
    NestedField(20, "garage_spaces", LongType(), False),
    NestedField(21, "garage_type", StringType(), False),
    NestedField(22, "heating_type", StringType(), False),
    NestedField(23, "cooling_type", StringType(), False),
    NestedField(24, "roof_type", StringType(), False),
    NestedField(25, "unit_count", LongType(), False),
    NestedField(26, "room_count", LongType(), False),
    NestedField(27, "architecture_type", StringType(), False),
    NestedField(28, "value_2023", LongType(), False),
    NestedField(29, "land_2023", LongType(), False),
    NestedField(30, "improvements_2023", LongType(), False),
    NestedField(31, "value_2024", LongType(), False),
    NestedField(32, "land_2024", LongType(), False),
    NestedField(33, "improvements_2024", LongType(), False),
    NestedField(34, "tax_2023", LongType(), False),
    NestedField(35, "owner_type", StringType(), False),
    NestedField(36, "owner_names", StringType(), False),
    NestedField(37, "owner_zip", StringType(), False),
    NestedField(38, "owner_city", StringType(), False),
    NestedField(39, "owner_state", StringType(), False),
    NestedField(40, "load_date", StringType(), False)
)

spec = PartitionSpec(PartitionField(source_id=40, field_id=102, transform="identity", name="load_date"))

# Main script
def main():
    offset = 0
    all_records = []

    for i in range(MAX_PAGES):
        logging.info(f"Fetching page {i+1} with offset={offset}")
        try:
            resp = requests.get(BASE_URL, headers=HEADERS, params={"limit": LIMIT, "offset": offset})
            resp.raise_for_status()
            page = resp.json()
        except Exception as e:
            logging.error(f"Failed at offset {offset}: {e}")
            break

        all_records.extend(page)
        if len(page) < LIMIT:
            break
        offset += LIMIT
        time.sleep(1)

    if not all_records:
        logging.warning("No data fetched from API. Exiting.")
        return

    logging.info(f"Fetched {len(all_records)} records. Writing to Iceberg...")
    transformed = [flatten_property(p) for p in all_records]
    arrow_table = pa.Table.from_pylist(transformed)

    catalog = load_catalog(
        name="academy",
        type="rest",
        uri="https://api.tabular.io/ws",
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret("TABULAR_CREDENTIAL")
    )

    if not catalog.table_exists(TABLE_NAME):
        catalog.create_table(TABLE_NAME, schema=schema, partition_spec=spec)
        logging.info("Created Iceberg table")

    table = catalog.load_table(TABLE_NAME)
    table.overwrite(arrow_table)
    logging.info("Data written to Iceberg.")

if __name__ == "__main__":
    main()

# This script fetches property data from the RentCast API, transforms it, and writes it to an Iceberg table.
# It handles pagination, error logging, and uses environment variables for configuration.