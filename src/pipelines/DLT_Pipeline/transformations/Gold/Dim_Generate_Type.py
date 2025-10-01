import dlt
from pyspark.sql import Row

environment = spark.conf.get("environment")
CATALOG = f"w_{environment}"

@dlt.table(
  name=f"{CATALOG}.gold.dim_generate_type",
  comment="Static dimension for power generation types (wind, solar)."
)
def dim_generate_type():
  """
  Creates a static dimension table with two rows for Solar and Wind generation types.
  """
  
  # Define the static data for the dimension table
  generation_types_data = [
    Row(dataset_id=248, generate_type="Solar", update_frequency="15 mins"),
    Row(dataset_id=245, generate_type="Wind", update_frequency="15 mins")
  ]
  
  # Create and return the Spark DataFrame.
  return spark.createDataFrame(generation_types_data)
