"""
Functions for Google Cloud BigQuery
"""

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class Megaton:
    """Class for Google Cloud BigQuery client
    """

    def __init__(self, credentials, project_id):
        self.id = project_id
        self.datasets = None
        self.credentials = credentials
        self.client = bigquery.Client(
            project=self.id,
            credentials=self.credentials
        )
        self.dataset = self.Dataset(self)
        self.table = self.Table(self)
        self.update()

    def update(self):
        """Get a list of dataset ids for the project"""
        # Make an API request.
        datasets = list(self.client.list_datasets())

        if datasets:
            # extract dataset id
            self.datasets = [d.dataset_id for d in datasets]
        else:
            print(f"project {self.id} does not have any datasets.")

    class Dataset:
        def __init__(self, parent):
            self.parent = parent
            self.ref = None
            self.id = None
            self.tables = None

        def select(self, id: str):
            if id:
                if id in self.parent.datasets:
                    if id != self.id:
                        self.update(id)
                else:
                    print(f"dataset {id} is not found in the project {self.parent.id}")
            else:
                self.id = None
                self.parent.table.id = None

        def update(self, dataset_id=None):
            """Get a list of table ids for the dataset"""
            id = dataset_id if dataset_id else self.id

            try:
                # call api
                dataset = self.parent.client.get_dataset(id)
                self.ref = dataset
                self.id = id
            except NotFound as e:
                if 'Not found: Dataset' in str(e):
                    print(f"Dataset {dataset_id} is not found in the project {self.parent.id}")
                return False

            # Make an API request.
            tables = list(self.parent.client.list_tables(dataset))

            if tables:
                # extract table id
                self.tables = [d.table_id for d in tables]
            else:
                print(f"dataset {self.id} does not have any tables.")

    class Table:
        def __init__(self, parent):
            self.parent = parent
            self.ref = None
            self.id = None

        def select(self, id: str):
            if id:
                if id in self.parent.dataset.tables:
                    if id != self.id:
                        self.update(id)
                else:
                    print(f"table {id} is not found in the dataset {self.parent.dataset.id}")
            else:
                self.id = None

        def update(self, table_id=None):
            """Get an api reference for a table"""
            id = table_id if table_id else self.id
            if self.parent.dataset.ref:
                try:
                    table = self.parent.dataset.ref.table(id)
                    self.ref = table
                    self.id = id
                except Exception as e:
                    raise e
            else:
                print("Please select a dataset first.")

        def create(self, table_id: str, schema: bigquery.SchemaField, partitioning_field: str = '',
                   clustering_fields=[]):
            dataset_ref = self.parent.dataset.ref
            table_ref = dataset_ref.table(table_id)
            table = bigquery.Table(table_ref, schema=schema)

            if partitioning_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partitioning_field,  # name of column to use for partitioning
                    expiration_ms=0,
                )
            if clustering_fields:
                table.clustering_fields = clustering_fields

            # Make an API request.
            table = self.parent.client.create_table(table_ref)

            print(f"Created table {table.table_id}", end='')
            if table.time_partitioning.field:
                print(f", partitioned on column {table.time_partitioning.field}")
            return table


def get_bq_schema(dict):
    schema = []
    for d in dict:
        schema.append(
            bigquery.SchemaField(
                name=d['name'],
                field_type=d['type'],
                description='',
            )
        )
    return schema
