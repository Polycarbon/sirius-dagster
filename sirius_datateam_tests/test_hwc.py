import unittest

from dagster import build_op_context

from sirius_datateam.jobs import hwc_resource_ingest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        result = hwc_resource_ingest.execute_in_process(build_op_context())
        print(result)
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
