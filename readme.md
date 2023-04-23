# EDA: Lightning Clustering Pipeline

An example performing `Exploratory Data Analysis` `(EDA)` by implementing simple K-Means clustering algorithm on a Lightning flash dataset collected from NOAA's GLM.
Uses [Dagster Software-Defined Assets](https://docs.dagster.io/concepts/assets/software-defined-assets) to orchestrate a Machine Learning pipeline.

Blog post: [Exploratory Data Analysis with Lightning Clustering Pipeline](https://medium.com/@adebayoadejare/exploratory-data-analysis-with-lightning-clustering-pipeline-6a2bca17d0d3)

<p align="center">
<figure align="center">
<img width="800px" alt="An example clustering of flash data points." src="screenshot/cluster_process.png">
<figcaption>Visual of clustering process</figcaption>
</figure>
</p>

## Installation

First make sure, you have the requirements installed, this can be installed from the project directory via pip's setup command:

`pip install .`

## Quick Start

Run the command to start the dagster orchestration framework: 

`dagster dev # Start dagster daemon and dagit ui`

The dagster daemon is required to start the scheduling, from the dagit ui, you can run and monitor the data assets.

## Data Ingestion

Ingests the data needed based on specified time window: start and end dates.

### Data Assets

+ `extract`: downloads [NOAA GOES-R GLM](https://www.goes-r.gov/spacesegment/glm.html) netCDF files from AWS s3 bucket
+ `transform`: converts GLM netCDF into time and geo series CSVs 
+ `load`: loads CSVs to a local backend, persistant duckdb

## Cluster Analysis

Performs grouping of the ingested data by implementing K-Means clustering algorithm.

### Data Assets

+ `ingestor`: Composed of `extract`, `transform`, and `load` data assets.
+ `preprocessor`: prepares the data for cluster model, clean and normalize the data.
+ `kmeans_cluster`: fits the data to an implementation of k-means cluster algorithm.
+ `evaluators`: two options for simple evaluations of the k-means cluster (unsupervised machine learning) model, one using `silhouette coefficient` and the other `elbow method`.

<p align="center">
<figure align="center">
<img width="400px" alt="Display of clustering materialized assets." src="screenshot/pipeline/eda_sda_pipe.png">
<figcaption>Dagit UI displaying EDA data assets</figcaption>
</figure>
</p>

## Testing

Use the following command to run tests:

`pytest`

## License

[Apache 2.0 License](LICENSE)