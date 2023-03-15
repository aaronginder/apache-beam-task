import json
import apache_beam as beam


class ProcessTransactions(beam.PTransform):
    """
    A class to read the transactions according to the structure of gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv, perform filtration and grouping tasks detailed in the project brief. The output of this class is a json object.
    """
    def __init__(self, file_delimiter: str = ',') -> beam.PCollection:
        self.file_delimiter = file_delimiter

    def expand(self, pcol: beam.PCollection):
        """
        Expand method of a beam class

        :param pcoll: pcollection object
        """
        transactions = (
            pcol
            | 'Parse CSV Data' >> beam.Map(lambda line: line.split(self.file_delimiter))
            | 'Filter Transactions' >> beam.Filter(lambda fields: float(fields[3]) > 20 and int(fields[0][:4]) >= 2010)
            | 'Create Output Structure' >> beam.Map(lambda fields: (fields[0], float(fields[3])))
            | 'Group by Date' >> beam.GroupByKey()
            | 'Calculate Total' >> beam.Map(lambda record: {'date': record[0], 'total': sum(record[1])})
            | 'Format to Json' >> beam.Map(json.dumps)
        )
        return transactions
