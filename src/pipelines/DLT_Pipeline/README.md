# Databricks DLT Pipeline

## Overview

This project contains a Delta Live Tables (DLT) pipeline designed to process energy data, including electricity consumption and power generation forecasts. The pipeline follows a medallion architecture (Bronze, Silver, Gold) to incrementally cleanse, transform, and model the data for business intelligence and analytics purposes.

The pipeline is dynamic and metadata-driven, capable of ingesting multiple datasets from an S3 landing zone, applying specific transformations based on dataset identifiers, and creating curated fact tables in the Gold layer.

## Pipeline Architecture

The pipeline is structured into three main layers, with an intermediate view layer to stage transformations before creating the final Gold tables.

![](https://i.imgur.com/your-image-url.png) <!--- Placeholder for your DLT graph image -->

### 1. Bronze Layer
Raw data is ingested from the source systems into the Bronze layer with minimal transformations.
*   **Source:** JSON files from an S3 landing zone (`s3a://wartsila-datalake-{environment}-landing/fingrid/`).
*   **Process:** The pipeline uses Auto Loader (`cloudFiles`) to read streaming data from S3.
*   **Tables:** A separate raw bronze table is dynamically created for each dataset specified in the pipeline configuration. These tables include metadata columns like `source_file` and `ingestion_timestamp`.
*   **Quality:** Basic data quality expectations are enforced, such as ensuring `startTime` and `endTime` are not null.

### 2. Silver Layer
The Silver layer cleanses and conforms the data, preparing it for business use cases.
*   **Process:** Data from Bronze tables is streamed into Silver tables. Dataset-specific transformation logic is applied to flatten JSON structures, rename columns, and cast data types to business-friendly formats.
*   **Logic:** A mapping dictionary (`TRANSFORMATION_LOGIC_MAP`) directs each dataset to its corresponding transformation function. For example, solar and wind power data are processed with `transform_power_generation`, while electricity consumption data uses `transform_consumption`.
*   **Tables:** Cleaned, structured tables for each dataset (e.g., `245_wind_power_generation`, `358_electricity_consumption`).

### 3. Intermediate View Layer
This layer acts as a staging area between the Silver and Gold layers. It uses DLT views (`@dlt.view`) to perform transformations, joins, and aggregations before loading the data into the final Gold tables. These views help modularize the logic and improve the readability of the pipeline.

The following views are defined in this layer:

*   **`pre_fact_consumption_view`**
    *   **Description:** This view reads the streaming data from the `silver.358_electricity_consumption` table and enriches it by joining with the `dim_date` and `dim_time` dimension tables.
    *   **Purpose:** It adds `start_date_id` and `start_time_id` surrogate keys, preparing the data for loading into the final `fact_consumption` table.
    *   **Source Table:** `silver.358_electricity_consumption`.
    *   **Target Table:** `fact_consumption`.

*   **`pre_fact_generation_forecast_view`**
    *   **Description:** This view unions the streams from the `silver.245_wind_power_generation` and `silver.248_solar_power_generation` tables to create a single, consolidated dataset for power generation.
    *   **Purpose:** It joins the combined generation data with `dim_date` and `dim_time` to add the required date and time surrogate keys before loading into the `fact_generation_forecast` table.
    *   **Source Tables:** `silver.245_wind_power_generation`, `silver.248_solar_power_generation`.
    *   **Target Table:** `fact_generation_forecast`.

*   **`pre_dim_customer_view`**
    *   **Description:** This view processes data to create a dimension table for customer types.
    *   **Purpose:** It is used to extract and prepare the data needed to populate the `dim_customer_type` table.
    *   **Source Table(s):** Likely `silver.358_electricity_consumption`.
    *   **Target Table:** `dim_customer_type`.

### 4. Gold Layer
The Gold layer aggregates data into final, business-level tables, such as fact and dimension tables, ready for BI dashboards and analytics.
*   **Process:** The intermediate views are used to populate the final Gold tables.
*   **Tables:**
    *   `fact_consumption`: Aggregates cleaned electricity consumption data.
    *   `fact_generation_forecast`: Combines wind and solar power generation data.
    *   `dim_customer_type`: Dimension table for customer types.
*   **Change Data Capture (CDC):** The pipeline uses the `dlt.create_auto_cdc_flow` function to automatically handle updates, deletes, and inserts, managing slowly changing dimensions (SCD Type 1) based on a sequence key like `bronze_ingestion_timestamp`.

## Notebooks

The pipeline is composed of the following notebooks, executed in order:

### `1_Ingestion_To_Bronze.py`
*   Reads pipeline parameters (`environment`, `datasets`).
*   Dynamically generates DLT table definitions for each dataset defined in the `datasets` parameter.
*   Uses Auto Loader to ingest raw JSON data from the specified S3 landing path into corresponding Bronze tables.

### `2_Bronze_To_Silver.py`
*   Reads data from the Bronze tables.
*   Applies dataset-specific transformation logic to clean, flatten, and typecast the data.
*   Creates Silver tables with a "quality" of "silver".

### `3_Fact_Consumption.py`
*   Creates the `pre_fact_consumption_view` by joining the `358_electricity_consumption` Silver table with date and time dimensions.
*   Uses this view to populate the `fact_consumption` Gold table, applying CDC logic.

### `3_Fact_Generation_Forecast.py`
*   Creates the `pre_fact_generation_forecast_view` by unioning wind and solar generation Silver tables and joining with dimension tables.
*   Loads the final data into the `fact_generation_forecast` Gold table using a CDC flow.

## Configuration

To run this DLT pipeline, you must configure the following pipeline settings in your Databricks job or DLT pipeline definition:

*   **`environment`**: The deployment environment (e.g., `dev`, `test`, `prod`). This parameter is used to construct the catalog name (e.g., `dev_wen`) and source S3 paths.
*   **`datasets`**: A JSON string representing a list of datasets to be ingested. Each object in the list must contain `datasetId` and `datasetName`.

**Example `datasets` configuration:**
[
{"datasetId": "248", "datasetName": "Solar power generation"},
{"datasetId": "245", "datasetName": "Wind power generation"},
{"datasetId": "358", "datasetName": "Electricity consumption"}
]
