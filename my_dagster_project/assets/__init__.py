import csv
import os

import requests
from dagster import asset, get_dagster_logger


@asset
def cereals():
    logger = get_dagster_logger()
    logger.debug("test log")
    logger.debug(os.getenv("AWS_ACCESS_KEY_ID","NO NAME"))
    logger.debug(os.getenv("AWS_SECRET_ACCESS_KEY", "NO NAME"))
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    cereal_rows = [row for row in csv.DictReader(lines)]

    return cereal_rows


@asset
def nabisco_cereals(cereals):
    """Cereals manufactured by Nabisco"""
    return [row for row in cereals if row["mfr"] == "N"]
