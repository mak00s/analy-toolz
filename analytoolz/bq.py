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
        self.for_ga4 = self.ForGA4(self)
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

    def run(self, query):
        """Run a SQL query and return data
        Args:
            query (str):
                SQL query to be executed.
        """
        job = self.client.query(query=query)
        results = job.result()  # Waits for job to complete.
        return results

    class Dataset:
        def __init__(self, parent):
            self.parent = parent
            self.ref = None
            self.instance = None
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
                self.ref = None
                self.instance = None
                self.id = None
                self.tables = None
                self.parent.table.select()

        def update(self, dataset_id=None):
            """Get a list of table ids for the dataset"""
            id = dataset_id if dataset_id else self.id

            try:
                dataset = self.parent.client.get_dataset(id)
                self.instance = dataset
                self.ref = dataset.reference
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
            self.instance = None
            self.id = None

        def _get_info(self):
            """Get metadata of the table"""

        def select(self, id: str):
            if id:
                if id in self.parent.dataset.tables:
                    if id != self.id:
                        self.update(id)
                else:
                    print(f"table {id} is not found in the dataset {self.parent.dataset.id}")
            else:
                self.ref = None
                self.instance = None
                self.id = None

        def update(self, table_id=None):
            """Get an api reference for a table"""
            id = table_id if table_id else self.id
            if self.parent.dataset.ref:
                try:
                    table_ref = self.parent.dataset.ref.table(id)
                    self.ref = table_ref
                    self.instance = self.parent.client.get_table(self.ref)
                    self.id = id
                    self._get_info()
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
                )
            if clustering_fields:
                table.clustering_fields = clustering_fields

            # Make an API request.
            table = self.parent.client.create_table(table)

            print(f"Created table {table.table_id}", end='')
            if table.time_partitioning.field:
                print(f", partitioned on column {table.time_partitioning.field}")
            self.parent.dataset.update()

            return table

    class ForGA4:
        """utilities to manage GA4"""

        def __init__(self, parent):
            self.parent = parent

        def create_clean_table(self, schema):
            """Create a table to store flatten GA data."""
            print(f"Creating a table to store flatten GA data.")
            # Make an API request.
            self.table.create(
                table_id='clean',
                schema=self.get_schema(schema),
                partitioning_field='date',
                clustering_fields=['client_id', 'event_name']
            )

        def get_schema(self, dict):
            """Convert a dictionary to BigQuery Schema"""
            schema = []
            for d in dict:
                schema.append(
                    bigquery.SchemaField(
                        name=d['name'],
                        field_type=d['type'],
                        description='test',
                    )
                )
            return schema

        def flatten_events(
                self,
                project_id,
                dataset,
                date1,
                date2,
                schema,
                event_parameters=[],
                user_properties=[]
        ):
            """Flatten event tables exported from GA4"""

            query = f'''--GA4 flatten events
                SELECT'''

            for s in schema:
                if s['Category']:
                    query += f'''
                    --{s['Category']}'''
                query += f'''
                    {s['Select']} AS {s['Field Name']},'''

            if user_properties:
                query += f'''
                    --Custom User Properties'''
                for d in user_properties:
                    query += f'''
                    (SELECT value.{d['type']}_value FROM UNNEST(user_properties) WHERE key = '{d['key']}') AS {d['field_name']},'''

            if event_parameters:
                query += f'''
                    --Custom Event Parameters'''
                for d in event_parameters:
                    query += f'''
                    (SELECT value.{d['type']}_value FROM UNNEST(event_params) WHERE key = '{d['key']}') AS {d['field_name']},'''

            query += f'''
                FROM
                    `{dataset}.events_*`
                WHERE
                    _TABLE_SUFFIX >= '{date1}' AND _TABLE_SUFFIX <= '{date2}'
                ORDER BY date, client_id, datetime'''

            return self.parent.run(query).to_dataframe()
