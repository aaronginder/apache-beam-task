import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from functions import ProcessTransactions

class TestProcessTransactions(unittest.TestCase):

    def setUp(self) -> None:
        self.correct_test_data = [
            "2015-01-01, origin_1, destination_1, 10.0",     #  Testing filter logic >20 transaction amount
            "2015-01-02, origin_2, destination_2, 30.0",
            "2015-01-01, origin_3, destination_3, 40.0",
            "2009-12-31, origin_4, destination_4, 25.0",     #  Testing filter logic >2010 year
            "2016-12-31, origin_5, destination_5, 15.0",     #  Testing filter logic >20 transaction amount
            "2015-01-03, origin_6, destination_6, 50.0"
        ]

    def test_process_transactions(self) -> None:
        # Expected output
        expected_output = [
            '{"date": "2015-01-02", "total": 30.0}',
            '{"date": "2015-01-01", "total": 40.0}',
            '{"date": "2015-01-03", "total": 50.0}'
        ]

        # Create a test pipeline
        with TestPipeline(runner='DirectRunner') as p:
            # Create a PCollection from the test data
            test_data = p | beam.Create(self.correct_test_data)

            # Apply the pipeline
            output = test_data | ProcessTransactions(file_delimiter=',')

            # Test the output
            assert_that(output, equal_to(expected_output))

if __name__ == '__main__':
    unittest.main()