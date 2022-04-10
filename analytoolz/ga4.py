"""
Functions for Google Analytics 4 API
"""

from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha.types import CustomDimension
from google.analytics.admin_v1alpha.types import CustomMetric
from google.analytics.admin_v1alpha.types import DataRetentionSettings
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Filter
from google.analytics.data_v1beta.types import FilterExpression
from google.analytics.data_v1beta.types import Metadata
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import MetricType
from google.analytics.data_v1beta.types import RunReportRequest


class LaunchGA4:
    def __init__(self, *args, **kwargs):
        """constructor"""
        self.credentials = kwargs.get('credentials')
        print("Creating GA4 Data client")
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)
        self.property_id = kwargs.get('property_id', None)

    def change_property(self, property_id):
        self.property_id = property_id

    def get_metadata(self):
        path = self.client.metadata_path(self.property_id)
        try:
            response = self.client.get_metadata(name=path)
        except Exception as e:
            print(e)
        else:
            dimensions = []
            for i in a.dimensions:
                dimensions.append({
                    'api_name': i.api_name,
                    'ui_name': i.ui_name,
                    'description': i.description,
                    'deprecated_api_names': i.deprecated_api_names,
                    'custom_definition': i.custom_definition,
                    'category': i.category})
            metrics = []
            for i in a.metrics:
                metrics.append({
                    'api_name': i.api_name,
                    'ui_name': i.ui_name,
                    'description': i.description,
                    'deprecated_api_names': i.deprecated_api_names,
                    'type': i.type_,
                    'expression': i.expression,
                    'custom_definition': i.custom_definition,
                    'blocked_reason': i.blocked_reason,
                    'category': i.category})
            return dimensions, metrics

    def get_data(self):
        response = self.client.run_report(request=request)


class GA4Admin():
    def __init__(self, *args, **kwargs):
        """constructor"""
        self.credentials = kwargs.get('credentials')
        print("Creating GA4 Admin client")
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
            list = []
            for item in results_iterator:
                dict = {
                    'id': self._get_account_id(item.account),
                    'name': item.display_name,
                    'properties': []}
                for p in item.property_summaries:
                    prop = {
                        'id': self._get_property_id(p.property),
                        'name': p.display_name
                    }
                    dict['properties'].append(prop)
                list.append(dict)
            return list

    def create_custom_dimension(self, property_id, parameter_name, display_name, description, scope='EVENT'):
        """Create custom dimension for the property."""
        try:
            self.client.create_custom_dimension(
                parent=f"properties/{property_id}",
                custom_dimension={
                    'parameter_name': parameter_name,
                    'display_name': display_name,
                    'description': description,
                    'scope': CustomDimension.DimensionScope[scope].value,
                }
            )
        except Exception as e:
            print(e)
        else:
            return True

    def custom_dimensions(self, property_id):
        """Returns custom dimensions for the property."""
        try:
            results_iterator = self.client.list_custom_dimensions(parent=f"properties/{property_id}")
        except Exception as e:
            print(e)
        else:
            list = []
            for item in results_iterator:
                dict = {
                    'display_name': item.display_name,
                    'parameter_name': item.parameter_name,
                    'description': item.description,
                    'scope': CustomDimension.DimensionScope(item.scope).name,
                }
                list.append(dict)
            return list

    def custom_metrics(self, property_id):
        """Returns custom metrics for the property."""
        try:
            results_iterator = self.client.list_custom_metrics(parent=f"properties/{property_id}")
        except Exception as e:
            print(e)
        else:
            list = []
            for item in results_iterator:
                dict = {
                    'display_name': item.display_name,
                    'parameter_name': item.parameter_name,
                    'description': item.description,
                    'scope': CustomDimension.DimensionScope(item.scope).name,
                    'measurement_unit': CustomMetric.MeasurementUnit(item.measurement_unit).name,
                    'restricted_metric_type': [CustomMetric.RestrictedMetricType(d).name for d in item.restricted_metric_type],
                }
                list.append(dict)
            return list

    def data_retention_settings(self, property_id):
        """Returns data retention settings for the property."""
        try:
            item = self.client.get_data_retention_settings(name=f"properties/{property_id}/dataRetentionSettings")
        except Exception as e:
            print(e)
        else:
                dict = {
                    'data_retention': DataRetentionSettings.RetentionDuration(item.event_data_retention).name,
                    'reset_user_data_on_new_activity': item.reset_user_data_on_new_activity,
                }
                return dict
