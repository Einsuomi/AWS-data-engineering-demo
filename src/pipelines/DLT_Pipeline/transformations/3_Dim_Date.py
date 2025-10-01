# Databricks notebook source
from datetime import date, timedelta
from math import ceil
import dlt

environment = spark.conf.get("environment")
CATALOG = f"w_{environment}"

@dlt.table(
  name=f"{CATALOG}.gold.dim_date",
  comment="Static date dimension from 2020 to 2050 with enriched features."
)
def dim_date():
    start_date, end_date = date(2020, 1, 1), date(2050, 12, 31)
    dates = []
    current = start_date
    while current <= end_date:
        quarter = ceil(current.month / 3)
        dates.append({
            "date": current,
            "date_id": int(current.strftime("%Y%m%d")),
            "year": current.year,
            "month": current.month,
            "day": current.day,
            "quarter": quarter,
            "day_name": current.strftime('%A'),
            "month_name": current.strftime('%B'),
            "day_of_week": current.weekday() + 1,  # Monday=1, Sunday=7
            "day_of_year": current.timetuple().tm_yday,
            "week_of_year": current.isocalendar()[1],
            "year_quarter": f"{current.year}-Q{quarter}",
            "year_month": current.strftime("%Y-%m"),
            "is_weekday": 1 if current.weekday() < 5 else 0,
            "is_weekend": 1 if current.weekday() >= 5 else 0,
            "is_month_start": 1 if current.day == 1 else 0,
            "is_month_end": 1 if (current + timedelta(days=1)).month != current.month else 0,
            "is_quarter_start": 1 if current.month in [1, 4, 7, 10] and current.day == 1 else 0,
            "is_year_start": 1 if current.month == 1 and current.day == 1 else 0,
            "is_year_end": 1 if current.month == 12 and current.day == 31 else 0
        })
        current += timedelta(days=1)
    
    return spark.createDataFrame(dates)
