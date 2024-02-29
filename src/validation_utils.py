from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id

from common_utils import create_logger
from load_utils import write_to_json

logger = create_logger()

UID_COLUMN_NAME = "_id"


def check_data_quality(df: DataFrame, batch: str) -> DataFrame:
    logger.info(f"Original dataframe count: {df.count()}")

    # Adding unique id for each record
    df_with_uid = df.withColumn(UID_COLUMN_NAME, monotonically_increasing_id())

    # Data Validation
    failed_uuid_set = set()

    failed_uuid_set.update(check_mandatory(df_with_uid))

    failed_uuid_set.update(check_format(df_with_uid))

    logger.info(f"Total failed record count: {len(failed_uuid_set)}")

    logger.info(failed_uuid_set)
    df_result = df_with_uid.where(~col(UID_COLUMN_NAME).isin(failed_uuid_set)).drop(
        UID_COLUMN_NAME
    )

    # Data Deduplication
    df_deduplicated = df_result.dropDuplicates()

    logger.info(
        f"Deduplicated record count: {df_result.count() - df_deduplicated.count()}"
    )

    # Write failed records to json file
    write_to_json(
        df_with_uid.where(col(UID_COLUMN_NAME).isin(failed_uuid_set)).drop(
            UID_COLUMN_NAME
        ),
        type="validation",
        subtype="Fail_Record",
        batch=batch,
    )

    # return valid records
    return df_deduplicated


def check_mandatory(df: DataFrame) -> set:
    # Mandatory Column Check
    MANDATORY_COLUMNS = [
        "givenName",
        "familyName",
        "sex",
        "dateOfBirth",
        "address.postcode",
    ]

    failed_man_uuid_set = set()
    for column in MANDATORY_COLUMNS:
        if column in df.columns:
            logger.debug(column)
            failed_man_uuid_set.update(
                df.where(col(column).isNull())
                .select(col(UID_COLUMN_NAME))
                .rdd.flatMap(list)
                .collect()
            )
            logger.debug(failed_man_uuid_set)

    logger.info(
        f"Failed Mandatory column checking record count: {len(failed_man_uuid_set)}"
    )

    return failed_man_uuid_set


def check_format(df: DataFrame) -> set:
    # Format Check: support Date, Postcode, Email
    FORMAT_MAPPING = [
        {
            "column": "dateOfBirth",
            "type": "date",
        },
        {
            "column": "address.postcode",
            "type": "postcode",
        },
        {
            "column": "email",
            "type": "email",
        },
    ]

    failed_format_uuid_set = set()
    for mapping in FORMAT_MAPPING:
        if mapping["type"] == "date":
            failed_format_uuid_set.update(
                df.where(
                    ~col(mapping["column"]).rlike(
                        r"[1-9][0-9][0-9]{2}-([0][1-9]|[1][0-2])-([1-2][0-9]|[0][1-9]|[3][0-1])"
                    )
                )
                .select(col(UID_COLUMN_NAME))
                .rdd.flatMap(list)
                .collect()
            )
            logger.info(f"Column {mapping['column']} check {failed_format_uuid_set}")
        elif mapping["type"] == "postcode":
            failed_format_uuid_set.update(
                df.where(
                    ~col(mapping["column"]).rlike(
                        r"[A-Z]{1,2}[0-9]{1,2}[A-Z]?\s?[0-9][A-Z]{2}"
                    )
                )
                .select(col(UID_COLUMN_NAME))
                .rdd.flatMap(list)
                .collect()
            )
            logger.info(f"Column {mapping['column']} check {failed_format_uuid_set}")
        elif mapping["type"] == "email":
            failed_format_uuid_set.update(
                df.where(
                    ~col(mapping["column"]).rlike(
                        r"^[\w!#$%&'*+/=?^_`{|}~-]+(?:\.[\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\w](?:[\w-]*[\w])?\.)+[\w](?:[\w-]*[\w])?$"
                    )
                )
                .select(col(UID_COLUMN_NAME))
                .rdd.flatMap(list)
                .collect()
            )
            logger.info(f"Column {mapping['column']} check {failed_format_uuid_set}")

    logger.info(f"Failed Format checking record count: {len(failed_format_uuid_set)}")

    return failed_format_uuid_set
