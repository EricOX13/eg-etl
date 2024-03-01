from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import create_map
from pyspark.sql.functions import lit
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import IntegerType
from itertools import chain

COUNTRY_MAPPING = {
    "E92000001": "England",
    "W92000004": "Wales",
    "S92000003": "Scotland",
    "N92000002": "Northern Ireland",
    "L93000001": "Channel Islands",
    "M83000003": "Isle of Man",
}


class PostcodeTransformer:

    @staticmethod
    def transform(df_source: DataFrame) -> DataFrame:

        if df_source is None:
            raise ValueError("Incorrect Postcode Value")

        df_postcode = df_source.select(
            regexp_replace(col("pcd"), " ", "").alias("postcode"),
            col("laua").alias("lacode"),
            col("ctry").alias("countryCode"),
            col("doterm").cast(IntegerType()).alias("date_of_termination"),
        )

        # replace country code by country name
        mapping_expr = create_map([lit(x) for x in chain(*COUNTRY_MAPPING.items())])

        df_postcode = df_postcode.withColumn(
            "country", mapping_expr[col("countryCode")]
        )

        return df_postcode.select(
            col("postcode"),
            col("lacode"),
            col("country"),
            col("date_of_termination"),
        )
