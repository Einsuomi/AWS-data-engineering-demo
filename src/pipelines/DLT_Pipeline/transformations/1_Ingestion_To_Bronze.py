# Databricks notebook source
import dlt
import json
import pyspark.sql.functions as F

# 1. Read pipeline parameters using spark.conf.get()
environment = spark.conf.get("environment")
datasets_str = spark.conf.get("datasets")
CATALOG = f"w_{environment}"

# 2. Parse the JSON string into a Python list of dictionaries
try:
    datasets = json.loads(datasets_str)
except (json.JSONDecodeError, TypeError):
    raise ValueError(f"Could not parse the 'datasets' pipeline parameter. Value received: {datasets_str}")

# 3. Define constants based on the environment
LANDING_ROOT = f"s3a://wartsila-datalake-{environment}-landing/fingrid/"

# 4. DEFINE THE FUNCTION
# This function takes a dataset's details and creates a unique DLT table definition for it.
def create_bronze_table_definition(dataset):
    dataset_name = dataset["name"]
    dataset_id = dataset["id"]

    # Sanitize the dataset name to create a valid table name
    table_name = f"{dataset_id}_{dataset_name.lower().replace(' ', '_')}_raw_table"
    
    # Define the specific path in the landing zone
    source_path = f"{LANDING_ROOT}/{dataset_id}_{dataset_name}/"
    
    # Expectations
    raw_expectations = {
                    "Rule1": "startTime IS NOT NULL",
                    "Rule2": "endTime IS NOT NULL"
                    }
    
    
    @dlt.table(
        name=f"{CATALOG}.bronze.{table_name}",
        comment=f"Raw bronze table for {dataset_id}_{dataset_name} from S3 landing zone.",
        table_properties={"quality": "bronze"}
    )
    @dlt.expect_all_or_drop(raw_expectations)
    
    def generated_bronze_table():
        """
        This is a dynamically generated function instance. DLT will discover it.
        It correctly captures table_name and source_path from its parent scope.
        """
        print(f"Defining streaming read for {table_name} from path {source_path}")
        return (
            spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", f"/dlt/schemas/{table_name}")
                .option("multiline", "true")
                .load(source_path)
                .select("*", 
                        F.col("_metadata.file_path").alias("source_file"),
                        F.current_timestamp().alias("ingestion_timestamp")
                        )
        )

# 5. CALL THE FUNCTION TO LOOP ALL DATASETS
for dataset in datasets:
    create_bronze_table_definition(dataset)
