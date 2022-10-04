from dagster import job, schedule, build_schedule_context, define_asset_job, AssetSelection

from sirius_datateam.assets import HUAWEI_CLOUD
from sirius_datateam.ops.cereal import hello_cereal, download_cereals, display_results, find_highest_calorie_cereal, \
    find_highest_protein_cereal

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