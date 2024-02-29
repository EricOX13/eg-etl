from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from common_utils import create_logger

logger = create_logger()


def write_to_json(df: DataFrame, type: str, subtype: str, batch: str):
    if df and df.isEmpty() is False:
        file_path = f"output/{type}/{subtype}_{batch}.json"
        logger.info(f"write to json path: {file_path}")
        df.write.format("json").save(f"output/{type}/{subtype}_{batch}.json")
        logger.info(f"write to json path: {file_path} completed!")


MERGE_DICT = {
    "users": {
        "match_columns": ["first_name", "last_name", "dob"],
        "update_columns": [
            "sex",
            "email",
            "address_street",
            "address_postcode",
        ],
    }
}


def write_to_iceberg(sc: SparkSession, df: DataFrame, table: str):
    if df and df.isEmpty() is False:
        df.createOrReplaceTempView("temp_source")
        settings = MERGE_DICT[table]
        match_clause = " and ".join(
            [f" s.{column} = t.{column}" for column in settings["match_columns"]]
        )
        update_clause = " , ".join(
            [f" t.{column} = s.{column}" for column in settings["update_columns"]]
        )
        query = f"""
            MERGE INTO db.{table} AS t
            USING (select * from temp_source) as s
            on {match_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT *
            """

        logger.debug(f"query: {query}")

        sc.sql(query)
        logger.info(f"Load to Datalake completed!")
