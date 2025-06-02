import sys

from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, upper
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType
import requests
spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']

api_key = 'Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q'
starter_url = f'https://api.polygon.io/v3/reference/tickers?limit=1000&apiKey={api_key}'
response = requests.get(starter_url)
tickers = []
data = response.json()
tickers.extend(data['results'])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Collect the results on the driver
while data['status'] == 'OK' and 'next_url' in data:
    tickers.extend(data['results'])
    print(data['next_url'] + '&apiKey=' + api_key)
    response = requests.get(data['next_url'] + '&apiKey=' + api_key)
    data = response.json()


schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("market", StringType(), False),
    StructField("active", BooleanType(), False),
    StructField("locale", StringType(), False),

    StructField("last_updated_utc", StringType(), True),
    StructField("name", StringType(), True),
    StructField("cik", StringType(), True),
    StructField("composite_figi", StringType(), True),
    StructField("currency_name", StringType(), True),
    StructField("primary_exchange", StringType(), True),
    StructField("share_class_figi", StringType(), True),
    StructField("type", StringType(), True)
])

date = datetime.strptime(run_date, "%Y-%m-%d").date()

#  enforce the schema
df = spark.createDataFrame(tickers, schema).withColumn('date', lit(date))



query = f"""

    CREATE TABLE IF NOT EXISTS {output_table} (
        active BOOLEAN,
        cik STRING,
        composite_figi STRING,
        currency_name STRING,
        last_updated_utc STRING,
        locale STRING,
        market STRING,
        name STRING,
        primary_exchange STRING,
        share_class_figi STRING,
        ticker STRING,
        `type` STRING,
        date DATE
        )
        USING iceberg
        PARTITIONED BY (date)  
        TBLPROPERTIES (
            'write.wap.enabled' = 'true'
        );       
"""

spark.sql(query)
(df.select(
    col("active"),
    col("cik"),
    col("composite_figi"),
    upper(col("currency_name")).alias("currency_name"),
    col("last_updated_utc"),
    col("locale"),
    col("market"),
    col("name"),
    col("primary_exchange"),
    col("share_class_figi"),
    col("ticker"),
    col("type"),
    col("date")
).dropDuplicates(['ticker']).writeTo(output_table).using("iceberg")
 .partitionedBy("date").overwritePartitions()
)
df.printSchema()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
