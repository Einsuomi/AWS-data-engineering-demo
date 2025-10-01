# Databricks notebook source
import dlt
import json
import pyspark.sql.functions as F
from pyspark.sql.types import *



# 1. Read pipeline parameters using spark.conf.get()
environment = spark.conf.get("environment")
datasets_str = spark.conf.get("datasets")
CATALOG = f"w_{environment}"

try:
    datasets = json.loads(datasets_str)
except (json.JSONDecodeError, TypeError):
    raise ValueError(f"Could not parse the 'datasets' pipeline parameter. Value received: {datasets_str}")

# 2. Define the specific transformation logic for each dataset
def transform_power_generation(df):
    """
    Transformation logic for the flat structure of solar and wind power generation datasets.
    """
    return (
        df.select(
            F.col("datasetId"),
            F.to_timestamp(F.col("startTime")).alias("start_time"),
            F.to_timestamp(F.col("endTime")).alias("end_time"),
            F.col("value").cast("double").alias("value_mwh"),
            F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_processed_timestamp")
        )
    )


def transform_consumption(df):
    """
    Transformation logic for the electricity consumption dataset.
    - Parses the 'additionalJson' string column.
    - Fully flattens all nested JSON fields into top-level columns.
    - Renames columns and casts data types to be business-friendly.
    """
    # Define the schema of the JSON string
    json_schema = StructType([
        StructField("TimeSeriesType", StringType(), True),
        StructField("Res", StringType(), True),
        StructField("Uom", StringType(), True),
        StructField("CustomerType", StringType(), True),
        StructField("ReadTS", StringType(), True),
        StructField("Value", StringType(), True), 
        StructField("Count", StringType(), True)
    ])
    
    return (
        df.withColumn("json_data", F.from_json(F.col("additionalJson"), json_schema))
          .select(
              F.col("datasetId"),
              F.to_timestamp(F.col("startTime")).alias("start_time"),
              F.to_timestamp(F.col("endTime")).alias("end_time"),
              F.col("value").cast("double").alias("consumption_mwh"),
              F.col("json_data.TimeSeriesType").alias("time_series_type"),
              F.col("json_data.Res").alias("resolution"),
              F.col("json_data.Uom").alias("unit_of_measure"),
              F.col("json_data.CustomerType").alias("customer_type"),
              F.to_timestamp(F.col("json_data.ReadTS")).alias("read_timestamp"),
              F.col("json_data.Value").cast("double").alias("json_value"),
              F.col("json_data.Count").cast("long").alias("reading_count"),
              F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
              F.current_timestamp().alias("silver_processed_timestamp")          )
    )

# 3. Create a mapping from dataset ID to its transformation function
TRANSFORMATION_LOGIC_MAP = {
    248: transform_power_generation, # Solar power
    245: transform_power_generation, # Wind power
    358: transform_consumption       # Electricity consumption
}

# 4. Define a function to create the DLT table definitions
def create_silver_table_definition(dataset, transform_function):
    """Dynamically creates a DLT silver table definition for a given dataset."""
    
    # Sanitize names for bronze and silver tables
    dataset_name = dataset["name"].lower().replace(' ', '_')
    dataset_id = dataset["id"]
    
    bronze_table_name = f"{dataset_id}_{dataset_name}_raw_table"
    silver_table_name = f"{dataset_id}_{dataset_name}"
    
    bronze_table_name = f"{CATALOG}.bronze.{bronze_table_name}"
    silver_table_name = f"{CATALOG}.silver.{silver_table_name}"

    @dlt.table(
        name = silver_table_name,
        comment = f"Cleaned silver table for {dataset['name']}.",
        table_properties={"quality": "silver"}
    )
    def generated_silver_table():
        # Read from the corresponding bronze table
        bronze_df = dlt.read_stream(bronze_table_name)
        
        return transform_function(bronze_df)

# 5. Loop through the datasets and create the silver tables
for dataset in datasets:
    dataset_id = dataset["id"]
    
    # Look up the correct transformation logic from the map
    transform_func = TRANSFORMATION_LOGIC_MAP.get(dataset_id)
    
    if transform_func:
        # Create the DLT table definition
        create_silver_table_definition(dataset, transform_func)
    else:
        print(f"Warning: No transformation logic found for dataset ID {dataset_id}. Skipping silver table creation.")

