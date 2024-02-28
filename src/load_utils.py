from pyspark.sql import DataFrame

from common_utils import create_logger

logger = create_logger()


def write_to_json(df: DataFrame, type: str, subtype: str, batch: str):
    if df and df.isEmpty() is False:
        file_path = f"output/{type}/{subtype}_{batch}.json"
        logger.info(f"write to json path: {file_path}")
        df.write.format("json").save(f"output/{type}/{subtype}_{batch}.json")
        logger.info(f"write to json path: {file_path} completed!")
