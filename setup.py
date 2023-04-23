from setuptools import find_packages, setup

setup(
    name="lightning_map",
    version="0.0.1",
    packages=find_packages(exclude=["lightning_map_tests"]),
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