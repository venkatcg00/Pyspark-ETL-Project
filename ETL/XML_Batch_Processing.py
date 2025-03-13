from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import (
    md5,
    concat_ws,
    col,
    concat,
    lit,
    when,
    to_timestamp,
    udf,
    coalesce,
    upper,
    row_number,
)
from pyspark.sql.window import Window
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine, MetaData, update, text
from sqlalchemy.orm import sessionmaker
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)
from DB_Lookup import (
    connect_to_database,
    return_lookup_value,
    close_database_connection,
)
import os
import configparser
import sys


def get_agent_id(agent_name, db_path):
    if agent_name is None:
        return None
    agent_name = f"'{agent_name}'"
    engine, Session = connect_to_database(db_path)
    agent_id = return_lookup_value(
        Session, "CSD_AGENTS", "'UBER'", "AGENT_ID", agent_name, "PSEUDO_CODE"
    )
    close_database_connection(engine)
    return agent_id


def get_support_area_id(support_area, db_path):
    if support_area is None:
        return None
    support_area = f"'{support_area}'"
    engine, Session = connect_to_database(db_path)
    support_area_id = return_lookup_value(
        Session,
        "CSD_SUPPORT_AREAS",
        "'UBER'",
        "SUPPORT_AREA_ID",
        support_area,
        "SUPPORT_AREA_NAME",
    )
    close_database_connection(engine)
    return support_area_id


def get_customer_type_id(customer_type, db_path):
    if customer_type is None:
        return None
    customer_type = f"'{customer_type}'"
    engine, Session = connect_to_database(db_path)
    customer_type_id = return_lookup_value(
        Session,
        "CSD_CUSTOMER_TYPES",
        "'UBER'",
        "CUSTOMER_TYPE_ID",
        customer_type,
        "CUSTOMER_TYPE_NAME",
    )
    close_database_connection(engine)
    return customer_type_id


def database_df_maker(db_path, source_id, spark):
    engine = create_engine(f"sqlite:///{db_path}")
    query = f"SELECT CSD_ID AS HISTORIC_CSD_ID, SOURCE_SYSTEM_IDENTIFIER AS HISTORIC_SSI, SOURCE_HASH_KEY AS HISTORIC_HASHKEY FROM CSD_DATA_MART WHERE ACTIVE_FLAG = 1 AND SOURCE_ID = {source_id}"
    pandas_df = pd.read_sql(query, con=engine)
    if pandas_df.empty:
        schema = StructType(
            [
                StructField("HISTORIC_CSD_ID", IntegerType(), True),
                StructField("HISTORIC_SSI", StringType(), True),
                StructField("HISTORIC_HASHKEY", StringType(), True),
            ]
        )
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    else:
        return spark.createDataFrame(pandas_df)


def duration_to_seconds(duration):
    if duration is None:
        return None
    else:
        parts = duration.split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])


duration_to_seconds_udf = udf(duration_to_seconds, IntegerType())


def xml_df_maker(xml_data, spark):
    schema = StructType(
        [
            StructField("SUPPORT_IDENTIFIER", StringType(), True),
            StructField("CONTACT_REGARDING", StringType(), True),
            StructField("AGENT_CODE", StringType(), True),
            StructField("DATE_OF_INTERACTION", StringType(), True),
            StructField("STATUS_OF_INTERACTION", StringType(), True),
            StructField("TYPE_OF_INTERACTION", StringType(), True),
            StructField("CUSTOMER_TYPE", StringType(), True),
            StructField("CONTACT_DURATION", StringType(), True),
            StructField("AFTER_CONTACT_WORK_TIME", StringType(), True),
            StructField("INCIDENT_STATUS", StringType(), True),
            StructField("FIRST_CONTACT_SOLVE", StringType(), True),
            StructField("TYPE_OF_RESOLUTION", StringType(), True),
            StructField("SUPPORT_RATING", StringType(), True),
            StructField("TIME_STAMP", StringType(), True),
        ]
    )

    records = []
    for record in xml_data:
        root = ET.fromstring(record[0])
        record_dict = {child.tag: child.text for child in root}
        records.append(record_dict)

    spark_df = spark.createDataFrame(records, schema)

    spark_df = spark_df.withColumn(
        "DATE_OF_INTERACTION",
        to_timestamp(col("DATE_OF_INTERACTION"), "yyyyMMddHHmmss"),
    )

    spark_df = spark_df.withColumn(
        "CONTACT_DURATION", duration_to_seconds_udf(col("CONTACT_DURATION"))
    )
    spark_df = spark_df.withColumn(
        "AFTER_CONTACT_WORK_TIME",
        duration_to_seconds_udf(col("AFTER_CONTACT_WORK_TIME")),
    )

    # Window specification to get the latest record for each SUPPORT_IDENTIFIER based on the highest value
    window_spec = Window.partitionBy("SUPPORT_IDENTIFIER").orderBy(
        col("SUPPORT_IDENTIFIER").desc()
    )

    # Add row number to each record within the partition
    df_final = spark_df.withColumn("row_num", row_number().over(window_spec))

    # Filter to get only the latest record for each TICKET_IDENTIFIER
    df_final = df_final.filter(col("row_num") == 1).drop("row_num")

    df_final = df_final.withColumn(
        "HASHKEY",
        md5(
            concat_ws("||", *[coalesce(col(c), lit("NULL")) for c in df_final.columns])
        ),
    )

    df_final = df_final.withColumn(
        "SUPPORT_IDENTIFIER", concat(lit("UBER - "), col("SUPPORT_IDENTIFIER"))
    )

    # Order by the first column (SUPPORT_IDENTIFIER)
    df_final = df_final.orderBy("SUPPORT_IDENTIFIER")

    return df_final


def data_transformer(database_df, xml_df, db_path, source_id, data_load_id):
    get_agent_id_udf = udf(
        lambda agent_name: get_agent_id(agent_name, db_path), StringType()
    )
    get_customer_type_id_udf = udf(
        lambda customer_type: get_customer_type_id(customer_type, db_path), StringType()
    )
    get_support_area_id_udf = udf(
        lambda support_area: get_support_area_id(support_area, db_path), StringType()
    )

    df = xml_df.join(
        database_df,
        xml_df["SUPPORT_IDENTIFIER"] == database_df["HISTORIC_SSI"],
        "left",
    )

    router_df = df.withColumn(
        "ROUTER_GROUP",
        when(col("HISTORIC_HASHKEY").isNull(), "INSERT")
        .when(col("HASHKEY") == col("HISTORIC_HASHKEY"), "DUPLICATE")
        .otherwise("UPDATE"),
    )

    filter_df = router_df.filter(col("ROUTER_GROUP") != "DUPLICATE")

    transformed_df = (
        filter_df.withColumn("SOURCE_ID", lit(source_id))
        .withColumn("SOURCE_SYSTEM_IDENTIFIER", col("SUPPORT_IDENTIFIER"))
        .withColumn("AGENT_ID", get_agent_id_udf(col("AGENT_CODE")))
        .withColumn("INTERACTION_DATE", col("DATE_OF_INTERACTION"))
        .withColumn(
            "SUPPORT_AREA_ID", get_support_area_id_udf(col("CONTACT_REGARDING"))
        )
        .withColumn("INTERACTION_STATUS", col("STATUS_OF_INTERACTION"))
        .withColumn("INTERACTION_TYPE", col("TYPE_OF_INTERACTION"))
        .withColumn("CUSTOMER_TYPE_ID", get_customer_type_id_udf(col("CUSTOMER_TYPE")))
        .withColumn("HANDLE_TIME", col("CONTACT_DURATION").cast(IntegerType()))
        .withColumn("WORK_TIME", col("AFTER_CONTACT_WORK_TIME").cast(IntegerType()))
        .withColumn(
            "FIRST_CONTACT_RESOLUTION",
            when(upper(col("FIRST_CONTACT_SOLVE")) == lit("TRUE"), 1)
            .when(upper(col("FIRST_CONTACT_SOLVE")) == lit("FALSE"), 0)
            .otherwise(None),
        )
        .withColumn("QUERY_STATUS", col("INCIDENT_STATUS"))
        .withColumn("SOLUTION_TYPE", col("TYPE_OF_RESOLUTION"))
        .withColumn("CUSTOMER_RATING", col("SUPPORT_RATING"))
        .withColumn("SOURCE_HASH_KEY", col("HASHKEY"))
        .withColumn("ROUTER_GROUP", col("ROUTER_GROUP"))
        .withColumn("HISTORIC_CSD_ID", col("HISTORIC_CSD_ID"))
        .withColumn("DATA_LOAD_ID", lit(data_load_id))
        .withColumn("START_DATE", lit(datetime.now()))
        .withColumn("END_DATE", lit(datetime.strptime("2099-12-31", "%Y-%m-%d")))
    )

    valid_record_check_df = transformed_df.withColumn(
        "IS_VALID_DATA",
        when(
            col("AGENT_ID").isNull()
            | col("INTERACTION_DATE").isNull()
            | col("SUPPORT_AREA_ID").isNull()
            | col("INTERACTION_STATUS").isNull()
            | col("INTERACTION_TYPE").isNull()
            | col("CUSTOMER_TYPE_ID").isNull()
            | col("HANDLE_TIME").isNull()
            | col("WORK_TIME").isNull()
            | col("FIRST_CONTACT_RESOLUTION").isNull()
            | col("QUERY_STATUS").isNull()
            | col("SOLUTION_TYPE").isNull()
            | col("CUSTOMER_RATING").isNull(),
            0,
        ).otherwise(1),
    )

    final_output_df = valid_record_check_df.select(
        "SOURCE_ID",
        "SOURCE_SYSTEM_IDENTIFIER",
        "AGENT_ID",
        "INTERACTION_DATE",
        "SUPPORT_AREA_ID",
        "INTERACTION_STATUS",
        "INTERACTION_TYPE",
        "CUSTOMER_TYPE_ID",
        "HANDLE_TIME",
        "WORK_TIME",
        "FIRST_CONTACT_RESOLUTION",
        "QUERY_STATUS",
        "SOLUTION_TYPE",
        "CUSTOMER_RATING",
        "SOURCE_HASH_KEY",
        "IS_VALID_DATA",
        "HISTORIC_CSD_ID",
        "ROUTER_GROUP",
        "DATA_LOAD_ID",
        "START_DATE",
        "END_DATE",
    )

    return final_output_df


def upsert_table(dataframe, db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables["CSD_DATA_MART"]

    pandas_df = dataframe.toPandas()

    total_upsert_count = 0
    valid_count = 0
    invalid_count = 0

    for index, row in pandas_df.iterrows():
        row_dict = row.to_dict()

        row_dict["INTERACTION_DATE"] = (
            row_dict["INTERACTION_DATE"].strftime("%Y-%m-%d %H:%M:%S")
            if pd.notna(row_dict["INTERACTION_DATE"])
            else None
        )
        row_dict["START_DATE"] = (
            row_dict["START_DATE"].strftime("%Y-%m-%d %H:%M:%S")
            if pd.notna(row_dict["START_DATE"])
            else None
        )
        row_dict["END_DATE"] = (
            row_dict["END_DATE"].strftime("%Y-%m-%d %H:%M:%S")
            if pd.notna(row_dict["END_DATE"])
            else None
        )

        mapped_row = {
            "SOURCE_SYSTEM_IDENTIFIER": row_dict.get("SOURCE_SYSTEM_IDENTIFIER"),
            "SOURCE_HASH_KEY": row_dict.get("SOURCE_HASH_KEY"),
            "SOURCE_ID": row_dict.get("SOURCE_ID"),
            "AGENT_ID": row_dict.get("AGENT_ID"),
            "INTERACTION_DATE": row_dict.get("INTERACTION_DATE"),
            "SUPPORT_AREA_ID": row_dict.get("SUPPORT_AREA_ID"),
            "INTERACTION_STATUS": row_dict.get("INTERACTION_STATUS"),
            "INTERACTION_TYPE": row_dict.get("INTERACTION_TYPE"),
            "CUSTOMER_TYPE_ID": row_dict.get("CUSTOMER_TYPE_ID"),
            "HANDLE_TIME": row_dict.get("HANDLE_TIME"),
            "WORK_TIME": row_dict.get("WORK_TIME"),
            "FIRST_CONTACT_RESOLUTION": row_dict.get("FIRST_CONTACT_RESOLUTION"),
            "QUERY_STATUS": row_dict.get("QUERY_STATUS"),
            "SOLUTION_TYPE": row_dict.get("SOLUTION_TYPE"),
            "CUSTOMER_RATING": row_dict.get("CUSTOMER_RATING"),
            "DATA_LOAD_ID": row_dict.get("DATA_LOAD_ID"),
            "IS_VALID_DATA": row_dict.get("IS_VALID_DATA"),
            "ACTIVE_FLAG": 1,
            "START_DATE": row_dict.get("START_DATE"),
            "END_DATE": row_dict.get("END_DATE"),
        }

        if row["ROUTER_GROUP"] == "INSERT":
            insert_stmt = table.insert().values(**mapped_row)
            session.execute(insert_stmt)
            total_upsert_count += 1
        elif row["ROUTER_GROUP"] == "UPDATE":
            deactivate_stmt = (
                update(table)
                .where(
                    table.c.CSD_ID == row["HISTORIC_CSD_ID"], table.c.ACTIVE_FLAG == 1
                )
                .values(ACTIVE_FLAG=0, END_DATE=row_dict.get("START_DATE"))
            )
            session.execute(deactivate_stmt)
            insert_stmt = table.insert().values(**mapped_row)
            session.execute(insert_stmt)
            total_upsert_count += 1

        if row_dict["IS_VALID_DATA"] == 1:
            valid_count += 1
        else:
            invalid_count += 1

    session.commit()
    session.close()

    return total_upsert_count, valid_count, invalid_count


def main(record_id, data_load_id):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    project_directory = os.path.dirname(current_directory)
    parameter_file_path = os.path.join(project_directory, "Setup", "Parameters.ini")

    config = configparser.ConfigParser()
    config.read(parameter_file_path)

    db_path = (
        f"{config.get('PATH', 'SQL_DB_PATH')}/{config.get('DATABASE', 'SQL_DB_NAME')}"
    )

    spark = SparkSession.builder.appName("XML Batch Processing").getOrCreate()

    engine, Session = connect_to_database(db_path)

    source_id = return_lookup_value(
        Session, "CSD_SOURCES", "'UBER'", "SOURCE_ID", "'UBER'", "SOURCE_NAME"
    )

    if source_id is None:
        print("Error: source_id is None. Exiting the script.")
        return

    close_database_connection(engine)

    historic_df = database_df_maker(db_path, source_id, spark)

    engine = create_engine(f"sqlite:///{db_path}")
    query = text(
        f"SELECT STREAMING_DATA FROM STREAMING_DATA_ARCHIVE A WHERE A.STREAM_RECORD_ID > {record_id} AND A.ARCHIVE_ID = (SELECT MAX(B.ARCHIVE_ID) FROM STREAMING_DATA_ARCHIVE B WHERE A.STREAM_RECORD_ID = B.STREAM_RECORD_ID)"
    )

    session = sessionmaker(bind=engine)
    with session() as s:
        result = s.execute(query)
        records = result.fetchall()

    if len(records) == 0:
        print("Error: No new records found. Exiting the script.")
        return {
            "TOTAL_UPSERT_COUNT": 0,
            "VALID_COUNT": 0,
            "INVALID_COUNT": 0,
            "DATA_VALID_PERCENTAGE": 0,
        }
    else:
        xml_data = records

    new_df = xml_df_maker(xml_data, spark)
    transformed_df = data_transformer(
        historic_df, new_df, db_path, source_id, data_load_id
    )

    total_upsert_count, valid_count, invalid_count = upsert_table(
        transformed_df, db_path
    )

    data_valid_percentage = (
        (valid_count / total_upsert_count) * 100 if total_upsert_count > 0 else 0
    )

    return {
        "TOTAL_UPSERT_COUNT": total_upsert_count,
        "VALID_COUNT": valid_count,
        "INVALID_COUNT": invalid_count,
        "DATA_VALID_PERCENTAGE": data_valid_percentage,
    }


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python XML_Batch_Processing.py <record_id> <data_load_id>")
        sys.exit(1)

    record_id = sys.argv[1]
    data_load_id = int(sys.argv[2])

    result = main(record_id, data_load_id)
    print(result)
