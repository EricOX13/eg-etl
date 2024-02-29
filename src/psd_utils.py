import gocept.pseudonymize

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import udf

from pyspark.sql.types import DateType
from pyspark.sql.types import StringType

SECRET = "secret"


def pseudonymize(df: DataFrame) -> DataFrame:
    """
    The pseudonymize data
    """
    nameUDF = udf(lambda z: gocept.pseudonymize.name(z, SECRET), StringType())
    dateUDF = udf(lambda z: gocept.pseudonymize.date(z, SECRET), DateType())
    emailUDF = udf(lambda z: gocept.pseudonymize.email(z, SECRET), StringType())
    streetUDF = udf(lambda z: gocept.pseudonymize.street(z, SECRET), StringType())
    df_pseudonymizeed = (
        df.withColumn("first_name", nameUDF(col("first_name")))
        .withColumn("last_name", nameUDF(col("last_name")))
        .withColumn("email", emailUDF(col("email")))
        .withColumn("address_street", streetUDF(col("address_street")))
        .withColumn("dob", dateUDF(col("dob")))
    )

    return df_pseudonymizeed
