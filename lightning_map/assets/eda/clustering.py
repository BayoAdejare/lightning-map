import os

import pandas as pd
import duckdb as db

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

def preprocess(lat: pd.DataFrame, lon: pd.DataFrame, context: str=None) -> pd.DataFrame:
    """Preprocess the data"""
    # drop file name
    lat.drop(lat.columns[[2]],axis=1,inplace=True)  
    lon.drop(lon.columns[[2]],axis=1,inplace=True) 
    # remove duplicates
    lat.drop_duplicates(subset=['ts_date'], inplace=True)
    lon.drop_duplicates(subset=['ts_date'], inplace=True)
    # join data
    geo_df = lon.join(lat.set_index('ts_date'), on='ts_date')

    return geo_df

def kmeans_model(data: pd.DataFrame, context: str=None):
    """
    Fit data to kmeans cluster algorithm.
    """
    k = 12
    X = data.loc[:, ["lon", "lat"]]
    kmeans = KMeans(n_clusters=k, n_init=10)
    X["Cluster"] = kmeans.fit_predict(X)
    X["Cluster"] = X["Cluster"].astype("category")
    return X

def sil_evaluation(data: pd.DataFrame, context: str=None):
    """
    Evaluate the k-means silhouette coefficient.
    """
    
    kmeans_kwargs = {
        "init": "random",
        "n_init": 10,
        "max_iter": 300,
        "random_state": 42,
    }

    # A list holds the silhouette coefficients for each k
    silhouette_coefficients = []

    # Notice we start at 2 clusters for silhouette coefficient
    for k in range(2, 24):
        kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
        kmeans.fit(data)
        score = silhouette_score(data, kmeans.labels_)
        silhouette_coefficients.append(score)
    
    return silhouette_coefficients

def elb_evaluation(data: pd.DataFrame, context: str=None):
    """
    Evaluate the k-means elbow method, sse.
    """
    
    kmeans_kwargs = {
        "init": "random",
        "n_init": 10,
        "max_iter": 300,
        "random_state": 42,
    }

    # A list holds the sum of squared distance for each k
    elb_sse = []

    # Return SSE for each k
    for k in range(1, 24):
        kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
        kmeans.fit(data)
        elb_sse.append(kmeans.inertia_)
    
    return elb_sse