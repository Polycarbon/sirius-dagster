import csv
import os

import requests
from dagster import asset, get_dagster_logger, load_assets_from_package_module

from sirius_datateam.assets import cereal

CEREAL = "cereal"
cereal_assets = load_assets_from_package_module(
    package_module=cereal, group_name=CEREAL
)
