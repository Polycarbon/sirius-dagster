import csv
import os

import requests
from dagster import asset, get_dagster_logger, load_assets_from_package_module, load_assets_from_package_name

from sirius_datateam.assets import huawei_cloud

HUAWEI_CLOUD = "huawei_cloud"

huawei_cloud_assets = load_assets_from_package_module(
    package_module=huawei_cloud, group_name=HUAWEI_CLOUD
)
