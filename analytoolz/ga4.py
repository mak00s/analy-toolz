"""
Functions for Google Analytics 4 API
"""

from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.data import BetaAnalyticsDataClient


class GA4Data():
    def __init__(self, *args, **kwargs):
        """constructor"""
        self.credentials = kwargs.get('credentials')
        print("Creating a client.")
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)


class GA4Admin():
    def __init__(self, *args, **kwargs):
        """constructor"""
        self.credentials = kwargs.get('credentials')
        self.client = AnalyticsAdminServiceClient(credentials=self.credentials)

    def _get_account_id(self, path: str):
        dict = self.client.parse_account_path(path)
        return dict.get('account')

    def _get_property_id(self, path: str):
        dict = self.client.parse_property_path(path)
        return dict.get('property')

    def account_summaries(self):
        """Returns summaries of all accounts accessible by the caller."""
        try:
            results_iterator = self.client.list_account_summaries()
        except Exception as e:
            print(e)
        else:
            accounts = []
            for item in results_iterator:
                act = {
                    'id': self._get_account_id(item.account),
                    'name': item.display_name,
                    'properties': []}
                for p in item.property_summaries:
                    prop = {
                        'id': self._get_property_id(p.property),
                        'name': p.display_name
                    }
                    act['properties'].append(prop)
                accounts.append(act)
            return accounts
