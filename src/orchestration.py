from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime
from extract_utils import extract_json
from constants import raw_users_schema
from validation_utils import check_data_quality
from transform.users import UserTransformer
from load_utils import write_to_iceberg

warehouse_path = "./warehouse"
iceberg_spark_jar = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3"
catalog_name = "demo"

# Setup iceberg config
conf = (
    SparkConf()
    .setAppName("EgApp")
    .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.jars.packages", iceberg_spark_jar)
    .set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
    .set(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
    .set("spark.sql.defaultCatalog", catalog_name)
)

sc = SparkSession.builder.master("local").config(conf=conf).getOrCreate()

batch = datetime.now().strftime("%Y%m%d_%H%M%S")
print(batch)


def main(file_path):

    # Extract User data from Json file
    df_users = extract_json(sc, file_path, raw_users_schema)

    # Data Validation
    df_validated = check_data_quality(df_users, batch)

    # Transformation
    df_transformed = UserTransformer.transform(df_validated)

    # Load to Data Lake
    write_to_iceberg(sc, df_transformed, "users")


if __name__ == "__main__":
    main("src_data/users.json")
