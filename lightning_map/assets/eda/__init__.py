import os
import pandas as pd
import duckdb as db

from datetime import datetime, date, timedelta
from dagster import asset, RetryPolicy
from .clustering import preprocess, kmeans_model, sil_evaluation, elb_evaluation
from .ingestor import ingestion

# Date range 
start_date = "2023-01-01"
end_date = "2023-01-02"

def db_connect(process: str):
    if process == "preprocess":
        # conn string for preprocess data
        conn = db.connect("data/Load/glmFlash.db")
        lat_df = conn.execute("SELECT * FROM tbl_flash_lat;").df() # latitude co-ordinates
        lon_df = conn.execute("SELECT * FROM tbl_flash_lon;").df() # longitude co-ordinates
        return lat_df, lon_df
    elif process == "model":
        # conn string for model data
        conn = db.connect("data/flashClusters.db")
        return conn


@asset(group_name="Ingest", description="Ingest data.", compute_kind="ml_ops")
def ingestor(context):
    context.log.info(f"Starting ingestion...")
    return ingestion(start_date, end_date, context)

@asset(group_name="EDA", description="Preprocess data.", compute_kind="prep", retry_policy=RetryPolicy(max_retries=3, delay=10))
def preprocessor(context, ingestor):
    # config data load
    lat_df, lon_df = db_connect(process="preprocess")
    context.log.info(f"Starting file extracts for lon: {lon_df}")
    context.log.info(f"Starting file extracts for lat: {lat_df}")
    results = []
    preprocessing = preprocess(lat_df, lon_df, context)
    results = pd.DataFrame(preprocessing)
    return results

@asset(group_name="EDA", description="Group data into 'k' clusters.", compute_kind="model")
def kmeans_cluster(context, preprocessor: pd.DataFrame):
    context.log.info(f"Starting cluster model: {preprocessor}")
    results = []
    clusters = kmeans_model(preprocessor, context)
    results = pd.DataFrame(clusters)
    context.log.info(f"Generated cluster model: {results}")
    # save clusters to db
    conn = db_connect(process="model")
    try:
        # create the table "cluster_analysis" from the DataFrame "results"
        conn.sql("CREATE TABLE cluster_analysis AS SELECT * FROM results")
    except Exception as db_insert:
        # insert into the table "cluster_analysis" from the DataFrame "results"
        conn.sql("INSERT INTO cluster_analysis SELECT * FROM results")
    return results

@asset(group_name="EDA", description="Silhoette coefficient score 'k'.", compute_kind="eval")
def Silhoette_evaluator(context, kmeans_cluster: pd.DataFrame):
    context.log.info(f"Starting silhoette evaluation: {kmeans_cluster}")
    results = []
    sil_coefficients = sil_evaluation(kmeans_cluster, context)
    results.append(sil_coefficients)
    context.log.info(f"Silhoutte coefficients: {results}")
    # save evaluations db
    return results

@asset(group_name="EDA", description="Elbow method score 'k'.", compute_kind="eval")
def elbow_evaluator(context, kmeans_cluster: pd.DataFrame):
    context.log.info(f"Starting elbow evaluation: {kmeans_cluster}")
    results = []
    elb_sse = elb_evaluation(kmeans_cluster, context)
    results.append(elb_sse)
    context.log.info(f"Elbow SSE: {results}")
    # save evaluations db
    return results