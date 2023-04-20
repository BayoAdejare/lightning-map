from setuptools import find_packages, setup

setup(
    name="etl_forecast",
    version="0.0.1",
    packages=find_packages(exclude=["etl_forecast_tests"]),
    install_requires=[
        "dagster",
        "duckdb",
        "netCDF4",
        "pandas",
        "boto3",
        "botocore",
    ],
    author='Adebayo Adejare',
    author_email='adebayo.adejare@gmail.com',
    extras_requires={"dev": ["dagit", "pytest"]},
)