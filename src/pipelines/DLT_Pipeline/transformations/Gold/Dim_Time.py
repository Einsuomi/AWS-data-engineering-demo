from datetime import date, timedelta
from math import ceil
import dlt

environment = spark.conf.get("environment")
CATALOG = f"w_{environment}"

@dlt.table(
  name=f"{CATALOG}.gold.dim_time",
  comment="Static time dimension in 15-minute intervals."
  )
def dim_time():
    time_quarters = []
    for hour in range(24):
        for quarter in range(4):
            minutes = quarter * 15
            time_quarters.append({
                "time_quarter_id": hour * 4 + quarter, "hour": hour, "quarter_of_hour": quarter,
                "minute": minutes, "time_15min": f"{hour:02d}:{minutes:02d}:00"
            })
    return spark.createDataFrame(time_quarters)
