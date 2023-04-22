import os
import shutil
import pandas as pd

from datetime import datetime, date, timedelta
from dagster import asset, RetryPolicy, MetadataValue
from .etl import extract, transform, load
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor

from botocore import UNSIGNED, exceptions
from botocore.client import Config
from boto3 import client
from pathlib import Path
from tqdm import tqdm

def etl_config(process: str):
    # Optional ETL parameters:
    dt = datetime.utcnow() - timedelta(hours=1)
    year = os.getenv("GOES_YEAR", dt.strftime('%Y'))
    day_of_year = os.getenv("GOES_DOY", dt.strftime('%j'))
    hour = os.getenv("GOES_HOUR", dt.strftime('%H'))
    # Required parameters:
    bucket_name = os.getenv("S3_BUCKET") # Satellite i.e. GOES-18  
    product_line = os.getenv("PRODUCT")  # Product line id i.e. ABI...
    prefix = f"{product_line}/{year}/{day_of_year}/{hour}/"
    basepath = Path(__file__).resolve().parent.parent.parent.parent
    if process == "extract":
        src_folder = prefix
        dest_folder = os.path.join(basepath, "data/Extract")
        # Create folder if not existing
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
    elif process == "transform":
        src_folder = os.path.join(basepath, "data/Extract")
        dest_folder = os.path.join(basepath, "data/transform")
        # Create folder if not existing
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
    elif process == "load": 
        src_folder = os.path.join(basepath, "data/transform")
        dest_folder = os.path.join(basepath, "data/Load")
        # Create folder if not existing
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
    else: 
        print(f"Process {process} not found!")

    return src_folder, bucket_name, dest_folder

@asset(group_name="ETL", description="Extract GOES netCDF files from s3 bucket.", compute_kind="s3 extract", retry_policy=RetryPolicy(max_retries=3, delay=10))
def source(context):
    # config file string
    prefix, bucket_name, extract_folder = etl_config(process="extract")
    # Navigate to folder
    os.chdir(extract_folder)
    results = []
    context.log.info(f"Starting file extracts for: {prefix}")
    # Configure s3 no sign in credential
    s3 = client('s3', config=Config(signature_version=UNSIGNED))
    # List existing files in buckets
    for files in tqdm(s3.list_objects(Bucket=bucket_name, Prefix=prefix)['Contents'], total=180, ascii=" >=", desc=f"Extract {prefix}"):
        # Download file from list
        filepath = files['Key']
        path, filename = os.path.split(filepath)
        print(f"Dowloading {filename} to {os.path.join(extract_folder, filename)}")
        s3_extract = extract(bucket_name, prefix, filename, filepath, context)
        results.append(s3_extract)
    # context.add_output_metadata({s3_extract -> results})
    return results
    
@asset(group_name="ETL", description="Convert GOES netCDF files into csv files.", compute_kind="transform data", retry_policy=RetryPolicy(max_retries=3, delay=10))
def transformations(context, source):
    # config file string
    extract_folder, bucket_name, transform_folder = etl_config(process="transform")
    glm_files = os.listdir(extract_folder)
    # Exit if source folder not existing
    if not os.path.exists(extract_folder):
        pass
    # Empty destination folder or recreate
    if not os.path.exists(transform_folder):
        os.makedirs(transform_folder)
    else:
        filelist = [ f for f in os.listdir(transform_folder) if f.endswith(".csv") ]
        for f in filelist:
            os.remove(os.path.join(transform_folder, f))
    results = []
    context.log.info(f"Starting file conversions for: {extract_folder}")
    # Convert glm files into one time series dataframe
    for filename in tqdm(glm_files, desc=f"transform {extract_folder}"):
        print(f"Converting {filename} to csv")
        csv_transform = transform(extract_folder, transform_folder, filename, context)
        results.append(csv_transform)
    # csv_transform -> results
    return results

@asset(group_name="ETL", description="Load GOES csv files into duckdb.", compute_kind="db load", retry_policy=RetryPolicy(max_retries=3, delay=10))
def destination(context, transformations):
    # config file string
    transform_folder, bucket_name, load_folder = etl_config(process="load")
    glm_files = os.listdir(transform_folder)
    context.log.info(f"Starting files load for: {transform_folder}")
    # Navigate to folder
    os.chdir(transform_folder)
    for filename in glm_files:
        try:
            # Copy to folder
            shutil.copy(filename, load_folder)
            # If source and destination are same
        except shutil.SameFileError:
            print("Source and destination represents the same file.")
        # If there is any permission issue
        except PermissionError:
            print("Permission denied.")
        # For other errors
        except:
            print(f"Error copying {filename} to {load_folder}.")
    results = []
    context.log.info(f"Loading {load_folder} bulk files to db.")
    db_load = load(load_folder, context)
    results.append(db_load)
    return results