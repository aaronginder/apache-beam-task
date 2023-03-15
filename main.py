import os
import apache_beam as beam
from functions import ProcessTransactions
from apache_beam.options.pipeline_options import PipelineOptions


# User Pipeline Options
class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--compression_ext",
                            type=str,
                            help="The compression extension of the file output. By default will compress file into .gz format",
                            dest="compression_ext",
                            default='gz',
                            required=False)

def execute_pipeline() -> None:
    options = PipelineOptions(runner='DirectRunner')
    user_options = options.view_as(UserOptions)
    file_name_suffix = f'.jsonl.{user_options.compression_ext}' if user_options.compression_ext != str() else '.jsonl'

    with beam.Pipeline(options=user_options) as pipeline:
        # Read Data from GCS
        data = pipeline |'Read CSV File' >> beam.io.ReadFromText(
            file_pattern='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
            skip_header_lines=1
        )

        # Composite PTransform to process transactions
        transactions = data | "Process Transactions" >> ProcessTransactions(file_delimiter=',')

        # Write the results to JSON files
        transactions | 'Write Output' >> beam.io.WriteToText(
            file_path_prefix='output/results',
            file_name_suffix=file_name_suffix,
            shard_name_template=str()
        )


if __name__ == '__main__':
    # Create output folder if does not exist
    if not os.path.exists('output'):
        os.makedirs('output')

    # Run pipeline
    execute_pipeline()
