import dlt
import pyspark.sql.functions as F

environment = spark.conf.get("environment")
CATALOG = f"w_{environment}"

@dlt.view(
    name="pre_fact_consumption_view"
    )

def pre_fact_consumption_view():
    df = (
        dlt.read_stream(f"{CATALOG}.silver.358_electricity_consumption")
        .withColumn("date", F.col("start_time").cast("date"))
        .join(dlt.read(f"{CATALOG}.gold.dim_date"), "date", "left")
        .withColumn(
            "time_15min",
            F.date_format(F.col("start_time"), "HH:mm:ss")
        )
        .join(dlt.read(f"{CATALOG}.gold.dim_time"), "time_15min", "left")
        .select(
            F.col("date_id").alias("start_date_id"),
            F.col("time_quarter_id").alias("start_time_id"),
            "datasetId",
            "consumption_mwh",
            "time_series_type",
            "resolution",
            "customer_type",
            "reading_count",
            "json_value",
            "bronze_ingestion_timestamp",
            F.current_timestamp().alias("gold_processed_timestamp")
        )
        )
    return df 


# --- Create Target Table for Fact Forecast ---
dlt.create_streaming_table(name=f"{CATALOG}.gold.fact_consumption")

# --- Auto CDC for fact_generation_forecast ---
dlt.create_auto_cdc_flow(
    target=f"{CATALOG}.gold.fact_consumption",
    source="pre_fact_consumption_view",
    keys=["start_date_id", "start_time_id", "customer_type"],
    sequence_by="bronze_ingestion_timestamp",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None,
    name = None,
    once = False
  )
