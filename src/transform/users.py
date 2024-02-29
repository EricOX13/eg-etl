from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from pyspark.sql.types import StringType

from psd_utils import pseudonymize
import uuid


class UserTransformer:

    @staticmethod
    def transform(df_source: DataFrame) -> DataFrame:

        if df_source is None:
            raise ValueError("Incorrect Users Value")

        uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())

        df_transformed_user = df_source.withColumn("id", uuidUdf()).select(
            col("id"),
            col("givenName").alias("first_name"),
            col("familyName").alias("last_name"),
            col("sex").alias("sex"),
            col("email").alias("email"),
            to_date(col("dateOfBirth"), "yyyy-MM-dd").alias("dob").cast(DateType()),
            col("address.street").alias("address_street"),
            col("address.postcode").alias("address_postcode"),
        )

        return pseudonymize(df_transformed_user)
