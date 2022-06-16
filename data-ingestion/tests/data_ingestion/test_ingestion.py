import pytest
import sys
import os

from shutil import rmtree
from pytest_mock.plugin import MockerFixture
from test_utils.pyspark import TestPySpark

import pandas as pd

from data_ingestion.ingestion import Ingester
from ingestion_expected import get_expected_metadata

class TestIngestion(TestPySpark):

    @classmethod
    def setup_class(cls):
        cls.spark = cls.start_spark()
        root_dir = os.path.dirname(os.path.realpath(__file__)).split("/data-ingestion/")[0]
        print('+++++++++  here +++++++++')
        print(root_dir)
        cls.parameters = {
            "co2_input_path":                   f"{root_dir}/datasets/ingestion/inputs/EmissionsByCountry.csv",
            "temperatures_global_input_path":   f"{root_dir}/datasets/ingestion/inputs/GlobalTemperatures.csv",
            "temperatures_country_input_path":  f"{root_dir}/datasets/ingestion/inputs/TemperaturesByCountry.csv",

            "co2_output_path":                  f"{root_dir}/data-ingestion/tmp/test/outputs/EmissionsByCountry.parquet/",
            "temperatures_global_output_path":  f"{root_dir}/data-ingestion/tmp/test/outputs/GlobalTemperatures.parquet/",
            "temperatures_country_output_path": f"{root_dir}/data-ingestion/tmp/test/outputs/TemperaturesByCountry.parquet/",
        }
        cls.ingester = Ingester(cls.spark, cls.parameters)
        return

    @classmethod
    def teardown_class(cls):
        cls.stop_spark()
        output_paths = cls.parameters.values()
        for path in output_paths:
            if ("/tmp/" in path) and os.path.exists(path):
                rmtree(path.rsplit("/", 1)[0])

    def test_replace_invalid_chars(self):
        # BEWARE: dictionaries do not necessarily enforce order.
        # To check column names, always use sorted()
        INVALID_CHARS = [" ", ",", ";", "\n", "\t", "=", "-", "{", "}", "(", ")"]
        df = pd.DataFrame(
            {
                'My Awesome Column': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
                '(Another) Awesome Column': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
            }
        )
        df.columns = [self.ingester.replace_invalid_chars(x) for x in df.columns]

        all_columns_valid = True
        for column in df.columns:
            if not all_columns_valid:
                break
            for char in INVALID_CHARS:
                if char in column:
                    all_columns_valid = False
                    break
        assert all_columns_valid
        assert sorted(df.columns) == sorted(["My_Awesome_Column", "Another_Awesome_Column"])

    def test_fix_columns(self):
        # BEWARE: dictionaries do not necessarily enforce order.
        # To check column names, always use sorted()
        pandas_df = pd.DataFrame(
            {
                'My Awesome Column': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
                '(Another) Awesome Column': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
            }
        )
        spark_df = self.spark.createDataFrame(pandas_df)
        fixed_df = self.ingester.fix_columns(spark_df)
        assert sorted(fixed_df.columns) == sorted(["My_Awesome_Column", "Another_Awesome_Column"])

    def test_run(self, mocker: MockerFixture):
        """High level job test: count + schema checks but nothing more granular"""

        # Optional - if mocking is needed:
        # mock_object = mocker.Mock()
        # mock_object.some_property.some_method(some_argument).another_method.return_value = ...

        # Run the job and check for _SUCCESS files for each partition
        self.ingester.run()

        output_path_keys = ["temperatures_country_output_path", "temperatures_global_output_path", "co2_output_path"]
        output_path_values = [self.parameters[k] for k in output_path_keys]
        expected_metadata_dict = get_expected_metadata()
        expected_metadata = [expected_metadata_dict[k.replace("_path", "")] for k in output_path_keys]

        for (path, expected) in list(zip(output_path_values, expected_metadata)):
            files = os.listdir(path)
            snappy_parquet_files = [x for x in files if x.endswith(".snappy.parquet")]
            # For this exercise, we require you to control each table's partitioning to 1 parquet partition
            assert (True if len(snappy_parquet_files) == 1 else False)
            assert (True if "_SUCCESS" in files else False)

            # Check count and schema - this covers most of pyspark-test's (https://pypi.org/project/pyspark-test/) functionality already
            # No need for a full equality check (it collects everything into the driver's memory - too time/memory consuming)
            df = self.spark.read.parquet(path)
            assert df.count() == expected["count"]
            assert df.schema == expected["schema"]

if __name__ == '__main__':
    pytest.main(sys.argv)
