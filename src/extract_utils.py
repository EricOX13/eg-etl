from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from common_utils import create_logger

logger = create_logger()


def read_lines(filename):
    with open(filename, "r") as f:
        for line in f:
            yield line


def extract_csv(sc: SparkSession, file_path: str) -> DataFrame:
    return sc.read.csv(file_path, header="true")


def extract_json(sc: SparkSession, file_path: str, schema) -> DataFrame:
    text = (
        sc.sparkContext.wholeTextFiles(file_path)
        .values()
        .flatMap(
            lambda x: x.replace("\n", "#!#")
            .replace("{#!# ", "{")
            .replace("#!#}", "}")
            .replace("#!#}", "}")
            .replace('"#!#', '"')
            .replace(",#!#", ",")
            .split("#!#")
        )
    )
    return sc.read.json(text, schema=schema, mode="PERMISSIVE", multiLine=True)
