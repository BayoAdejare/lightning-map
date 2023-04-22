from dagster import IOManager, Definitions, ScheduleDefinition, define_asset_job, load_assets_from_package_module

from .assets import etl, eda

etl_asset_job = define_asset_job(name="etl_job", selection=["source", "transformations", "destination"])
hourly_etl_schedule = ScheduleDefinition(
    job=etl_asset_job,
    cron_schedule="@hourly",
    execution_timezone="America/New_York"
)

ingestion_asset_job = define_asset_job(name="ingestion_job", selection="ingestor")
clustering_job = define_asset_job(name="clustering_job", selection=["ingestor", "preprocessor", "kmeans_cluster", "Silhouette_evaluator"])
hourly_clustering_schedule = ScheduleDefinition(
    job=clustering_job,
    cron_schedule="@hourly",
    execution_timezone="America/New_York"
)

# Data assets definitions
defs = Definitions(
    assets=load_assets_from_package_module(assets), 
    jobs=[etl_asset_job, ingestion_asset_job, clustering_job],
    # resources: s3, io_manager
    schedules=[hourly_etl_schedule, hourly_clustering_schedule],
)