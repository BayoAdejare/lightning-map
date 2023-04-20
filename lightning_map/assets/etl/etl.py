#!/usr/bin/env python

import os
import shutil

import netCDF4 as nc
import pandas as pd
import duckdb as db

from botocore.client import Config
from botocore import UNSIGNED, exceptions
from boto3 import client
from tqdm import tqdm
from pathlib import Path

def extract(bucket: str, prefix: str, filename: str,  filepath: str, context: str=None) -> pd.DataFrame:
    """
    Downloads GOES netCDF files from s3 buckets
    prefix = s3://<weather_satellite>/<product_line>/<year>/<day_of_year>/<hour>/<OR_...*.nc>
    """
    s3 = client('s3', config=Config(signature_version=UNSIGNED))
    try:
        s3.download_file(Bucket=bucket,Filename=filename,Key=filepath)
    except exceptions.ClientError as err:
        if err.response['Error']['Code'] == "404":
            print(f"{filename} cannot be located.")
        else:
            raise
    # List files downloaded
    df_extract = pd.DataFrame(os.listdir())
    return df_extract
    

def transform(extract_folder: str, transform_folder: str, filename: str, context: str=None) -> pd.DataFrame: 
    """
    Convert GOES netCDF files into csv
    """
    file_conn = Path(os.path.join(extract_folder, filename))
    energy_filename = file_conn.with_suffix('').with_suffix('.ene.csv') # energy file
    lat_filename = file_conn.with_suffix('').with_suffix('.lat.csv') # latitude file
    lon_filename = file_conn.with_suffix('').with_suffix('.lon.csv') # longitude file
    # Create dataset
    glm =  nc.Dataset(file_conn, mode='r')
    flash_lat = glm.variables['flash_lat'][:]
    flash_lon = glm.variables['flash_lon'][:]
    flash_time = glm.variables['flash_time_offset_of_first_event']
    flash_energy = glm.variables['flash_energy'][:]
    dtime = nc.num2date(flash_time[:],flash_time.units)
    # Flatten multi-dimensional data into series        
    flash_energy_ts = pd.Series(flash_energy, index=dtime)
    flash_lat_ts = pd.Series(flash_lat, index=dtime)
    flash_lon_ts = pd.Series(flash_lon, index=dtime)
    # Headers 
    with open(energy_filename, 'w') as glm_file:
        glm_file.write('ts_date,energy\n')
    # Write to csv
    flash_energy_ts.to_csv(energy_filename, index=True, header=False, mode='a')
    # Headers 
    with open(lat_filename, 'w') as glm_file:
        glm_file.write('ts_date,lat\n')
    # Write to csv
    flash_lat_ts.to_csv(lat_filename, index=True, header=False, mode='a')
    with open(lon_filename, 'w') as glm_file:
        glm_file.write('ts_date,lon\n')
    # Write to csv
    flash_lon_ts.to_csv(lon_filename, index=True, header=False, mode='a')
    # Move files
    shutil.move(energy_filename, transform_folder) 
    shutil.move(lat_filename, transform_folder) 
    shutil.move(lon_filename, transform_folder) 
    # List converted files
    df_transform = pd.DataFrame(os.listdir())
    return df_transform

def load(load_folder: str, context: str=None) -> pd.DataFrame:
    """
    Load GOES csv files into destination system
    """ 
    glm_files = [s for s in os.listdir() if s.endswith('.csv')]    
    conn = db.connect(f"{load_folder}/glmFlash.db")
    os.chdir(load_folder)
    try:
        # create the table "tbl_flash" from the csv files
        conn.execute(f"""
            CREATE TABLE tbl_flash 
            AS 
            SELECT 
                *
            FROM read_csv_auto('*.ene.csv', header=True, filename=True, types={{'ts_date': TIMESTAMP,'energy': DOUBLE}});
            """)
    except Exception as db_insert:
        # table likely exist try insert
        conn.execute(f"INSERT INTO tbl_flash SELECT * FROM read_csv_auto('*.ene.csv', header=True, filename=True);")

    try:
        # create the table "tbl_flash_lat" from the csv files
        conn.execute(f"""
            CREATE TABLE tbl_flash_lat 
            AS 
            SELECT 
                *
            FROM read_csv_auto('*.lat.csv', header=True, filename=True, types={{'ts_date': TIMESTAMP,'lat': DOUBLE}});
            """)
    except Exception as db_insert:
        # table likely exist try insert
        conn.execute(f"INSERT INTO tbl_flash_lat SELECT * FROM read_csv_auto('*.lat.csv', header=True, filename=True);")

    try:
        # create the table "tbl_flash_lon" from the csv files
        conn.execute(f"""
            CREATE TABLE tbl_flash_lon 
            AS 
            SELECT 
                *
            FROM read_csv_auto('*.lon.csv', header=True, filename=True, types={{'ts_date': TIMESTAMP,'lon': DOUBLE}});
            """)
    except Exception as db_insert:
        # table likely exist try insert
        conn.execute(f"INSERT INTO tbl_flash_lon SELECT * FROM read_csv_auto('*.lon.csv', header=True, filename=True);")

    os.makedirs('loaded')
    # try:
    for filename in glm_files:
        # Move loaded files
        shutil.move(filename, "loaded")
    # except Exception as e:
        # print("Exception errors received.")
    # cleanup
    shutil.rmtree('loaded')
    # List files loaded
    df_load = conn.execute("SHOW TABLES;").df()
    return df_load