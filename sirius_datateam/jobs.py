from dagster import job, schedule, build_schedule_context, define_asset_job, AssetSelection, get_dagster_logger, \
    RunRequest, ScheduleEvaluationContext
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from sirius_datateam.assets import HUAWEI_CLOUD
from sirius_datateam.ops.cereal import hello_cereal, download_cereals, display_results, find_highest_calorie_cereal, \
    find_highest_protein_cereal
from sirius_datateam.ops.huawei_cloud import get_token, get_all_resources, to_json_file, upload_s3, \
    huawei_cloud_accounts

huawei_job = define_asset_job(
    "huawei_cloud_ingestion",
    selection=AssetSelection.groups(HUAWEI_CLOUD),
)


@job
def hello_cereal_job():
    """Example of a simple Dagster job."""
    hello_cereal()


@job
def complex_job():
    """Example of a more complex Dagster job."""
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
    )


@job(resource_defs={
    "s3": s3_resource.configured(
        {"endpoint_url": "http://obs.ap-southeast-2.myhuaweicloud.com"}
    )
})
def hwc_resource_ingest():
    """Example of a more complex Dagster job."""
    accounts = huawei_cloud_accounts()
    tokens = get_token(accounts)
    resources_paths = tokens.map(get_all_resources)
    resources_paths.map(upload_s3)
    # print(resources_paths)
    # path_result = resource_gen.map(to_json_file)
    # path_result.map(upload_s3)


@schedule(job=hwc_resource_ingest, cron_schedule="0 1 * * *")
def at_1am_daily(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config={
            "ops": {"upload_s3": {"config": {"scheduled_date": scheduled_date}}}
        },
        tags={"date": scheduled_date},
    )
