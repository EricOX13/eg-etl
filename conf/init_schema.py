from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

warehouse_path = "./warehouse"
iceberg_spark_jar = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0"
catalog_name = "evergreen"


# Setup iceberg config
def init_database(spark: SparkSession):

    # Create database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS db")

    # Write and read Iceberg table
    users_table_name = "db.users"
    if not spark.catalog.tableExists(users_table_name):
        users_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("sex", StringType(), True),
                StructField("email", StringType(), True),
                StructField("dob", DateType(), True),
                StructField("address_street", StringType(), True),
                StructField("address_postcode", StringType(), True),
            ]
        )
        df = spark.createDataFrame([], users_schema)
        df.writeTo(users_table_name).create()

    # Write and read Iceberg table
    postcode_table_name = "db.postcode"
    if not spark.catalog.tableExists(postcode_table_name):
        postcode_schema = StructType(
            [
                StructField("postcode", StringType(), True),
                StructField("lacode", StringType(), True),
                StructField("country", StringType(), True),
                StructField("date_of_termination", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame([], postcode_schema)
        df.writeTo(postcode_table_name).create()
