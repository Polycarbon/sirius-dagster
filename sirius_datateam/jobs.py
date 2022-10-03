from dagster import job

from sirius_datateam.ops.hwc_api import get_all_resources, get_token


@job
def hwc_resource_ingest():
    get_all_resources(get_token())
