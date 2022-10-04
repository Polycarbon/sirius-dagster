from dagster import job, schedule, build_schedule_context, define_asset_job, AssetSelection

from sirius_datateam.assets import HUAWEI_CLOUD

huawei_job = define_asset_job(
        "huawei_cloud_ingestion",
        selection=AssetSelection.groups(HUAWEI_CLOUD),
    )