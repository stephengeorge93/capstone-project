from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv
from aws_secret_manager import get_secret
# Load environment variables from .env file
load_dotenv()

catalog = load_catalog(
    name="academy",
    type="rest",
    uri="https://api.tabular.io/ws",
    warehouse=get_secret("CATALOG_NAME"),
    credential=get_secret("TABULAR_CREDENTIAL")
)

TABLE_NAME = "stephengeorge93.rentcast_properties"

if catalog.table_exists(TABLE_NAME):
    catalog.drop_table(TABLE_NAME)
    print("✅ Table dropped")
else:
    print("⚠️ Table does not exist")
