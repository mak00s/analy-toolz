"""
Functions for Google Cloud BigQuery
"""

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class Megaton:
    """Class for Google Cloud BigQuery client
    """

    def __init__(self, credentials, project):
        self.credentials = credentials
        self.project = project
        self.datasets = None
        self.client = bigquery.Client(
            project=self.project,
            credentials=self.credentials
        )
        self.get_datasets()
        self.dataset = self.Dataset(self)
        self.table = self.Table(self)

    def get_datasets(self):
        datasets = list(self.client.list_datasets())

        if datasets:
            self.datasets = [d.dataset_id for d in datasets]
        else:
            print(f"{self.project} does not have any datasets.")

    class Dataset:
        def __init__(self, parent):
            self.parent = parent
            self.id = None

        def select(self, id: str):
            if id:
                if id != self.id and id in self.parent.datasets:
                    self.id = id
                    self.update()
            else:
                self.parent.table.id = None

    class Table:
        def __init__(self, parent):
            self.parent = parent
            self.id = None

        def select(self, id: str):
            if id:
                if id != self.id:
                    self.id = id
                    self.update()
