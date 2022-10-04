from dagster import job, schedule

from sirius_datateam.assets.huawei_cloud import get_all_resources, get_token


@job
def hwc_resource_ingest():
    get_all_resources(get_token())


@schedule(cron_schedule="* * * * *", job=hwc_resource_ingest, execution_timezone="US/Central")
def my_schedule(_context):
    return {}
