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
        df.withColumn("givenName", nameUDF(col("givenName")))
        .withColumn("familyName", nameUDF(col("familyName")))
        .withColumn("email", emailUDF(col("email")))
        .withColumn("address.street", streetUDF(col("address.street")))
        .withColumn("dateOfBirth", dateUDF(col("dateOfBirth")))
    )

    return df_pseudonymizeed
