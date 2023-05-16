import os

import pandas as pd
import duckdb as db

from dagster import asset, RetryPolicy
from .clustering import preprocess, kmeans_model, sil_evaluation, elb_evaluation
from .ingestor import ingestion
from datetime import datetime, timedelta

# Date range
dt = datetime.utcnow() - timedelta(hours=1)
start_date = str(dt)
end_date = str(dt)
hours = dt.strftime('%H')

# 24 hours
# hours = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", 
#             "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"]


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


@asset(group_name="Ingest", description="Ingest data.", compute_kind="etl")
def ingestor(context):
    context.log.info(f"Starting ingestion from {start_date} to {end_date}..")
    return ingestion(start_date, end_date, hours, context)

@asset(group_name="Cluster", description="Preprocess data.", compute_kind="prep", retry_policy=RetryPolicy(max_retries=3, delay=10))
def preprocessor(context, ingestor):
    # config data load
    lat_df, lon_df = db_connect(process="preprocess")
    context.log.info("Starting file extracts for lon ...")
    context.log.info("Starting file extracts for lat ...")
    results = []
    preprocessing = preprocess(lat_df, lon_df, context)
    results = pd.DataFrame(preprocessing)
    return results

@asset(group_name="Cluster", description="Group data into 'k' clusters.", compute_kind="model")
def kmeans_cluster(context, preprocessor: pd.DataFrame):
    k = int(os.getenv("NUM_OF_CLUSTERS", 12))
    context.log.info(f"Starting cluster model, k={k}...")
    results = []
    clusters = kmeans_model(preprocessor, k, context)
    results = pd.DataFrame(clusters)
    context.log.info(f"Generated cluster model ...")
    # save clusters to db
    conn = db_connect(process="model")
    try:
        # create the table "cluster_analysis" from the DataFrame "results"
        conn.sql("CREATE TABLE cluster_analysis AS SELECT * FROM results")
    except Exception as db_insert:
        # insert into the table "cluster_analysis" from the DataFrame "results"
        conn.sql("INSERT INTO cluster_analysis SELECT * FROM results")
    return results

@asset(group_name="Cluster", description="Silhouette coefficient score 'k'.", compute_kind="eval", retry_policy=RetryPolicy(max_retries=3, delay=10))
def Silhouette_evaluator(context, kmeans_cluster: pd.DataFrame):
    context.log.info(f"Starting silhouette evaluation ...")
    sil_coefficients = sil_evaluation(kmeans_cluster, context)
    results = sil_coefficients.set_index('k', drop=True)
    k_max = results['silhouette_coefficient'].argmax()
    context.log.info(f"Silhoutte coefficients: {results}")
    os.environ["NUM_OF_CLUSTERS"] = str(k_max)
    # save evaluations db
    return results

@asset(group_name="Cluster", description="Elbow method score 'k'.", compute_kind="eval")
def elbow_evaluator(context, kmeans_cluster: pd.DataFrame):
    context.log.info(f"Starting elbow evaluation: {kmeans_cluster}")
    results = []
    elb_sse = elb_evaluation(kmeans_cluster, context)
    results.append(elb_sse)
    context.log.info(f"Elbow SSE ...")
    # save evaluations db
    return results