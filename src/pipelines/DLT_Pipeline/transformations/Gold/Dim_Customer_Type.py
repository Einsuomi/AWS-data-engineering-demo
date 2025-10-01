import dlt
import pyspark.sql.functions as F

environment = spark.conf.get("environment")
CATALOG = f"w_{environment}"

@dlt.view(
    name = "pre_dim_customer_type_view"
)
def pre_dim_customer_type_view():
    df = (dlt.read_stream(f"{CATALOG}.silver.358_electricity_consumption")
          .select(F.col("customer_type"),
                  F.col("bronze_ingestion_timestamp")
                  )
          )
    return df

dlt.create_streaming_table(
    name = f"{CATALOG}.gold.dim_customer_type"
)

dlt.create_auto_cdc_flow(
  target = f"{CATALOG}.gold.dim_customer_type",
  source = "pre_dim_customer_type_view",
  keys = ["customer_type"],
  sequence_by = "bronze_ingestion_timestamp",
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
