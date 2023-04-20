# Lightning Events Map: Clustering EDA

An example performing `Exploratory Data Analysis` `(EDA)` by implementing simple K-Means clustering algorithm on a Lightning flash dataset collected from NOAA's GLM.
Uses [Dagster Software-Defined Assets](https://docs.dagster.io/concepts/assets/software-defined-assets) to orchestrate a Machine Learning pipeline.

Blog post: Coming soon!

<p align="center">
<figure>
<img width="30%" src="screenshot/pipeline/eda_sda_pipe.png">
<figcaption>Dagit UI displaying EDA data assets</figcaption>
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

Two required parameters, S3_BUCKET and PRODUCT, are configured by default. There is current support for three optional paramters for loading specific available files: GOES_YEAR, GOES_DOY and GOES_HOUR

`export GOES_YEAR=2022; export GOES_DOY=365; export GOES_HOUR=05 # Run the pipeline for Dec 31, 2022 5 AM files`

## Data Assets

+ `ingestor`: process downloads [NOAA GOES-R GLM](https://www.goes-r.gov/spacesegment/glm.html) files from AWS s3 bucket, converts them into time and geo series CSVs and loads them to a local backend, persistant duckdb. Composed of `extract`, `transform`, and `load` data assets.
+ `preprocessor`: prepares the data for cluster model, clean and normalize the data.
+ `kmeans_cluster`: fits the data to an implementation of k-means cluster algorithm.
+ `evaluators`: two options for simple evaluations of the unsupervised machine learning model, one using silhoette coefficient and elbow method.


## Testing

Use the following command to run tests:

`pytest`

## License

[Apache 2.0 License](LICENSE)