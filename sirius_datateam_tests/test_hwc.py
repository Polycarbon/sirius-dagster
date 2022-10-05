import unittest

from dagster import build_op_context

from sirius_datateam.jobs import hwc_resource_ingest


class HwcJobTestCase(unittest.TestCase):
    def test_hwc_resource_ingest(self):

        result = hwc_resource_ingest.execute_in_process().success

        self.assertTrue(result)  # add assertion here


if __name__ == '__main__':
    unittest.main()
