from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

raw_users_schema = StructType(
    [
        StructField("email", StringType(), True),
        StructField("givenName", StringType(), True),
        StructField("familyName", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("dateOfBirth", StringType(), True),
        StructField(
            "address",
            StructType(
                [
                    StructField("street", StringType(), True),
                    StructField("postcode", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)
