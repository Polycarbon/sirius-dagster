from dagster import build_op_context
from dagster._core.test_utils import instance_for_test

from sirius_datateam.jobs import hwc_resource_ingest


def test_asset_with_service():
    # When invoking asset_with_service, service resource will
    # automatically be used.
    # context = build_op_context()
    # result = hwc_resource_ingest.execute_in_process()
    with instance_for_test() as instance:
        assert (
            hwc_resource_ingest
            .execute_in_process(instance=instance,run_config={
            "ops": {"upload_s3": {"config": {"scheduled_date": "2021-08-02"}}}
        })
            .success
        )

if __name__ == '__main__':
    test_asset_with_service()