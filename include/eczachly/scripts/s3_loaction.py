from pyiceberg.catalog import load_catalog
from aws_secret_manager import get_secret
from dotenv import load_dotenv

load_dotenv()

def main():
    catalog = load_catalog(
        name="academy",
        type="rest",
        uri="https://api.tabular.io/ws",
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret("TABULAR_CREDENTIAL")
    )

    table = catalog.load_table("stephengeorge93.rentcast_properties")

    print(table)  

if __name__ == "__main__":
    main()



'Table S3 URI: s3://zachwilsonsorganization-522/ce557692-2f28-41e8-8250-8608042d2acb/d77dbb91-4a2c-4ccc-a4e0-e42da35d253d'