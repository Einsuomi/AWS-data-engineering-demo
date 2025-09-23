import dlt
from pyspark.sql.functions import col, explode, current_timestamp, lit, row_number, when, split, regexp_extract
from pyspark.sql.window import Window
from datetime import date, timedelta
from math import ceil

# =====================================================================================
# Configuration
# =====================================================================================
LANDING_ROOT = "s3a://wartsila-datalake-dev-landing/fingrid/"

CATALOG = "w_dev"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# Define dataset details
datasets_config = {
    "solar_forecast": {
        "id": 248,
        "path": f"{LANDING_ROOT}fingrid_solar_power_generation_forecast_updated_every_15_minutes/",
        "type": "Solar"
    },
    "wind_forecast": {
        "id": 245,
        "path": f"{LANDING_ROOT}fingrid_wind_power_generation_forecast_updated_every_15_minutes/",
        "type": "Wind"
    },
    "electricity_consumption": {
        "id": 358,
        "path": f"{LANDING_ROOT}Electricity_consumption_by_customer_type/",
        "type": "Consumption"
    }
}

# =====================================================================================
# BRONZE LAYER
# Ingest raw data into separate tables for each source.
# =====================================================================================
def create_bronze_table(name, path):
    @dlt.table(
        name=f"bronze_{name}",
        # --- FIX: Set the table properties to define catalog and schema ---
        table_properties={"pipelines.target.database": BRONZE_SCHEMA}
    )
    def bronze_table():
        return (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", f"/mnt/dlt/checkpoints/bronze_{name}")
                .load(path)
                .select(
                    explode("data").alias("record"),
                    current_timestamp().alias("ingestion_timestamp"),
                    "_metadata"
                )
        )
# Dynamically create a bronze table for each dataset
for name, config in datasets_config.items():
    create_bronze_table(name, config["path"])

# =====================================================================================
# SILVER LAYER
# Clean, flatten, and structure the data from each bronze table.
# =====================================================================================
@dlt.table(
    name="silver_solar_forecast",
    table_properties={"pipelines.target.database": SILVER_SCHEMA}
)
def silver_solar_forecast():
    return (
        dlt.read_stream(f"{BRONZE_SCHEMA}.bronze_solar_forecast")
        .select(
            col("record.startTime").cast("TIMESTAMP").alias("start_time"),
            col("record.endTime").cast("TIMESTAMP").alias("end_time"),
            col("record.value").cast("DOUBLE").alias("value"),
            lit(datasets_config["solar_forecast"]["id"]).alias("dataset_id"),
            "ingestion_timestamp"
        )
    )

@dlt.table(
    name="silver_wind_forecast",
    table_properties={"pipelines.target.database": SILVER_SCHEMA}
)
def silver_wind_forecast():
    return (
        dlt.read_stream(f"{BRONZE_SCHEMA}.bronze_wind_forecast")
        .select(
            col("record.startTime").cast("TIMESTAMP").alias("start_time"),
            col("record.endTime").cast("TIMESTAMP").alias("end_time"),
            col("record.value").cast("DOUBLE").alias("value"),
            lit(datasets_config["wind_forecast"]["id"]).alias("dataset_id"),
            "ingestion_timestamp"
        )
    )

@dlt.table(
    name="silver_electricity_consumption",
    table_properties={"pipelines.target.database": SILVER_SCHEMA}
)
def silver_electricity_consumption():
    return (
        dlt.read_stream(f"{BRONZE_SCHEMA}.bronze_electricity_consumption")
        .select(
            col("record.startTime").cast("TIMESTAMP").alias("start_time"),
            col("record.endTime").cast("TIMESTAMP").alias("end_time"),
            col("record.value").cast("DOUBLE").alias("value"),
            col("record.customer_type").alias("customer_type"),
            col("record.time_series_type").alias("time_series_type"),
            col("record.res").alias("res"),
            lit(datasets_config["electricity_consumption"]["id"]).alias("dataset_id"),
            "ingestion_timestamp"
        )
    )

# # =====================================================================================
# # GOLD LAYER - DIMENSIONS
# # Create the static and derived dimension tables.
# # =====================================================================================
# @dlt.table(name="dim_date", comment="Static date dimension from 2020 to 2050.", table_properties={"pipelines.target.database": GOLD_SCHEMA})
# def dim_date():
#     start_date, end_date = date(2020, 1, 1), date(2050, 12, 31)
#     dates = []
#     current = start_date
#     while current <= end_date:
#         dates.append({
#             "date": current, "date_id": int(current.strftime("%Y%m%d")), "year": current.year,
#             "month": current.month, "day": current.day, "day_of_week": current.weekday() + 1,
#             "day_name": current.strftime('%A'), "month_name": current.strftime('%B'),
#             "quarter": ceil(current.month / 3), "is_weekday": 1 if current.weekday() < 5 else 0
#         })
#         current += timedelta(days=1)
#     return spark.createDataFrame(dates)

# @dlt.table(name="dim_time", comment="Static time dimension in 15-minute intervals.", table_properties={"pipelines.target.database": GOLD_SCHEMA})
# def dim_time():
#     time_quarters = []
#     for hour in range(24):
#         for quarter in range(4):
#             minutes = quarter * 15
#             time_quarters.append({
#                 "time_quarter_id": hour * 4 + quarter, "hour": hour, "quarter_of_hour": quarter,
#                 "minute": minutes, "time_15min": f"{hour:02d}:{minutes:02d}:00"
#             })
#     return spark.createDataFrame(time_quarters)

# @dlt.table(name="dim_customer", comment="Dimension for electricity customer types.", table_properties={"pipelines.target.database": GOLD_SCHEMA})
# def dim_customer():
#     return (
#         dlt.read(f"{SILVER_SCHEMA}.silver_electricity_consumption")
#         .select("customer_type", "time_series_type", "res")
#         .dropDuplicates()
#         .withColumn('customerID', row_number().over(Window().orderBy(lit(1))).cast('bigint'))
#     )

# @dlt.table(name="dim_generate_type", comment="Dimension for power generation types (wind, solar).",table_properties={"pipelines.target.database": GOLD_SCHEMA})

# def dim_generate_type():
#     # Combine forecast streams to create the dimension
#     solar_df = dlt.read(f"{SILVER_SCHEMA}.silver_solar_forecast").select("dataset_id").dropDuplicates()
#     wind_df = dlt.read(f"{SILVER_SCHEMA}.silver_wind_forecast").select("dataset_id").dropDuplicates()

#     return (
#         solar_df.union(wind_df)
#         .withColumn("generate_type",
#             when(col("dataset_id") == 248, "Solar")
#             .when(col("dataset_id") == 245, "Wind")
#             .otherwise("Other")
#         )
#         .withColumn("update_frequency", lit("15 mins"))
#     )

# # =====================================================================================
# # GOLD LAYER - FACTS
# # Create the fact tables using DLT's `apply_changes` for SCD Type 1 logic.
# # =====================================================================================
# # --- Fact Consumption ---
# dlt.create_target_table(
#     name="fact_consumption",
#     table_properties={"pipelines.target.database": GOLD_SCHEMA}
# )

# consumption_stream_df = (
#     dlt.read_stream(f"{SILVER_SCHEMA}.silver_electricity_consumption")
#     .join(dlt.read(f"{GOLD_SCHEMA}.dim_customer"), ["customer_type", "time_series_type", "res"], "left")
#     .withColumn("date", col("start_time").cast("date"))
#     .join(dlt.read(f"{GOLD_SCHEMA}.dim_date"), "date", "left")
#     .withColumn("time_15min", regexp_extract(col("start_time").cast("string"), r"(\d{2}:\d{2}:\d{2})", 1))
#     .join(dlt.read(f"{GOLD_SCHEMA}.dim_time"), "time_15min", "left")
#     .select(
#         "date_id",
#         col("time_quarter_id").alias("start_time_id"),
#         "customerID",
#         "value",
#         "ingestion_timestamp"
#     )
# )

# dlt.apply_changes(
#     # FIX: Use the fully qualified name for the target
#     target="fact_consumption", 
#     source=consumption_stream_df,
#     keys=["date_id", "start_time_id", "customerID"],
#     sequence_by="ingestion_timestamp",
#     stored_as_scd_type=1
# )

# # --- Fact Forecast ---
# dlt.create_target_table(
#     name="fact_forecast",
#     table_properties={"pipelines.target.database": GOLD_SCHEMA}
# )

# forecast_stream_df = (
#     dlt.read_stream(f"{SILVER_SCHEMA}.silver_solar_forecast")
#     # FIX: Added a dot between the schema and table name
#     .unionByName(dlt.read_stream(f"{SILVER_SCHEMA}.silver_wind_forecast"))
#     .withColumn("date", col("start_time").cast("date"))
#     .join(dlt.read(f"{GOLD_SCHEMA}.dim_date"), "date", "left")
#     .withColumn("time_15min", regexp_extract(col("start_time").cast("string"), r"(\d{2}:\d{2}:\d{2})", 1))
#     .join(dlt.read(f"{GOLD_SCHEMA}.dim_time"), "time_15min", "left")
#     .select(
#         "date_id",
#         col("time_quarter_id").alias("start_time_id"),
#         "dataset_id",
#         "value",
#         "ingestion_timestamp"
#     )
# )

# dlt.apply_changes(
#     # FIX: Use the fully qualified name for the target
#     target="fact_forecast",
#     source=forecast_stream_df,
#     keys=["date_id", "start_time_id", "dataset_id"],
#     sequence_by="ingestion_timestamp",
#     stored_as_scd_type=1
# )