
import os 

import pandas as pd
from dagster import materialize
from ..etl import source, transformations, destination

def ingestion_config():
    # Optional ETL parameters:
    year = os.getenv("GOES_YEAR")
    day_of_year = os.getenv("GOES_DOY")
    hour = os.getenv("GOES_HOUR")
    # Required parameters:
    bucket_name = os.getenv("S3_BUCKET") # Satellite i.e. GOES-18  
    product_line = os.getenv("PRODUCT")  # Product line id i.e. ABI...
    prefix = product_line + '/' + year + '/' + day_of_year + '/' + hour + '/'

    return prefix, bucket_name

# 24 hours
# hours = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", 
            #    "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"]
hours = ["00", "01"]

def ingestion(start_date: str, end_date: str, context: str=None):
    """Collects the data"""

    for single_date in pd.date_range(start_date, end_date, freq="D"):
        hours_bucket = hours
        for single_hour in hours_bucket:
            print(f"Start date: {start_date}; End date: {end_date}")
            # Export env vars
            os.environ["GOES_YEAR"] = single_date.strftime("%Y")
            os.environ["GOES_DOY"] = single_date.strftime("%j")
            os.environ["GOES_HOUR"] = str(single_hour)
            # config file string
            prefix, bucket_name = ingestion_config()
            print(f"Prefix: {prefix}; Bucket: {bucket_name}")
            try:
                # Extract files
                materialize([source, transformations, destination])
            except Exception as e:
                print(f"Error extracting files from {prefix}")
