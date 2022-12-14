from dagster import define_asset_job, load_assets_from_package_module, repository, with_resources, AssetsDefinition
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource, s3_file_manager

from sirius_datateam import assets
from sirius_datateam.assets import  huawei_cloud_assets
from sirius_datateam.jobs import huawei_job, hello_cereal_job, complex_job, hwc_resource_ingest, at_1am_daily


@repository
def sirius_datateam():
    # When you define a downstream asset that depends on another asset, to take advantage of Dagster's
    # incremental re-execution functionality (e.g. retry from failure), you'll need to set up an IO
    # manager that can move the data across runs. Learn more at https://docs.dagster.io/concepts/io-management/io-managers#applying-io-managers-to-assets
    #
    # NOTE: In this starter project, we provide a commented example that uses an s3_pickle_io_manager to
    # pass data between runs. To get started:
    # 1. Uncomment the code below.
    # 2. Replace "MY_S3_BUCKET" and "MY_S3_PREFIX" with your own values.
    #   * This example also varies s3 prefix based on environments. Read more about branch deployment
    #   best practices at https://docs.dagster.io/guides/dagster/branch_deployments
    # 3. Provide the "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" environment variables if you
    # are accessing your private buckets.
    #   * Learn about adding secrets in Serverless at https://docs.dagster.io/dagster-cloud/deployment/serverless#adding-secrets

    # if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT"):
    #     s3_prefix = f"MY_S3_PREFIX/branch_{os.getenv('DAGSTER_CLOUD_PULL_REQUEST_ID')}"
    # else:
    #     s3_prefix = "MY_S3_PREFIX"

    s3_prefix = "test"
    resource_defs = {
        "io_manager": s3_pickle_io_manager.configured(
            {"s3_bucket": "sirius-dagster", "s3_prefix": s3_prefix}
        ),
        "s3": s3_resource.configured(
            {"endpoint_url": "http://obs.ap-southeast-2.myhuaweicloud.com"}
        ),
    }
    all_assets = [*huawei_cloud_assets]
    all_jobs = [
        huawei_job,
        complex_job,
        hwc_resource_ingest,
        at_1am_daily
    ]
    return [
        with_resources(
            definitions=all_assets, resource_defs=resource_defs
        ),
        # define_asset_job(name="all_assets_job"),
        all_jobs
    ]

    # return [
    #     load_assets_from_package_module(assets),
    #     define_asset_job(name="all_assets_job"),
    # ]
if __name__ == '__main__':
    sirius_datateam()