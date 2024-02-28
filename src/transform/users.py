from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import create_map
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
from itertools import chain


class UserTransformer:

    @staticmethod
    def transform(df_source: DataFrame) -> DataFrame:

        if df_source is None:
            raise ValueError("Incorrect Postcode Value")

        df_postcode = df_source.select(
            col("pcd").alias("postcode"),
            col("laua").alias("lacode"),
            col("ctry").alias("countryCode"),
            col("doterm").cast(IntegerType()).alias("dateOfTermination"),
        )

        return df_postcode.select(
            col("postcode"),
            col("lacode"),
            col("country"),
            col("dateOfTermination"),
        )
