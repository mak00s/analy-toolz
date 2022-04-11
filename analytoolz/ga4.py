"""
Functions for Google Analytics 4 API
"""

from datetime import datetime
import pytz

from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha.types import CustomDimension
from google.analytics.admin_v1alpha.types import CustomMetric
from google.analytics.admin_v1alpha.types import DataRetentionSettings
from google.analytics.admin_v1alpha.types import IndustryCategory
from google.analytics.admin_v1alpha.types import ServiceLevel
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
    def __init__(self, credentials, *args, **kwargs):
        """constructor"""
        print("Creating GA4 Data client")
        self.credentials = credentials
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)
        self.property_id = None
        if kwargs.get('property_id'):
            self.set_property(kwargs.get('property_id'))

    def set_property(self, property_id):
        self.property_id = property_id

    def available_dimensions_and_metrics(self):
        path = self.client.metadata_path(self.property_id)
        try:
            response = self.client.get_metadata(name=path)
        except Exception as e:
            print(e)
        else:
            dimensions = []
            for i in response.dimensions:
                dimensions.append({
                    'customized': i.custom_definition,
                    'category': i.category,
                    'api_name': i.api_name,
                    'display_name': i.ui_name,
                    'description': i.description,
                    # 'deprecated_api_names': i.deprecated_api_names,
                })
            metrics = []
            for i in response.metrics:
                metrics.append({
                    'customized': i.custom_definition,
                    'category': i.category,
                    'api_name': i.api_name,
                    'display_name': i.ui_name,
                    'description': i.description,
                    # 'deprecated_api_names': i.deprecated_api_names,
                    'type': i.type_,
                    'expression': i.expression,
                })
            return dimensions, metrics

    def get_data(self):
        response = self.client.run_report(request=request)


class ManageGA4:
    def __init__(self, credentials, *args, **kwargs):
        """constructor"""
        print("Creating GA4 Admin client")
        self.credentials = credentials
        self.client = AnalyticsAdminServiceClient(credentials=self.credentials)
        self.property_id = None
        if kwargs.get('property_id'):
            self.set_property(kwargs.get('property_id'))

    def _extract_account_id(self, path: str):
        dict = self.client.parse_account_path(path)
        return dict.get('account')

    def _extract_property_id(self, path: str):
        dict = self.client.parse_property_path(path)
        return dict.get('property')

    def set_property(self, property_id):
        self.property_id = property_id

    def accounts(self):
        """Returns summaries of all accounts accessible by the caller."""
        try:
            results_iterator = self.client.list_account_summaries()
        except Exception as e:
            print(e)
        else:
            list = []
            for item in results_iterator:
                dict = {
                    'id': self._extract_account_id(item.account),
                    'name': item.display_name,
                    'properties': [],
                }
                for p in item.property_summaries:
                    prop = {
                        'id': self._extract_property_id(p.property),
                        'name': p.display_name
                    }
                    dict['properties'].append(prop)
                list.append(dict)
            return list

    def properties(self, account_id):
        """Returns summaries of all properties belonging to the account"""
        try:
            results_iterator = self.client.list_properties({
                'filter': f"parent:accounts/{account_id}",
                'show_deleted': False,
            })
        except Exception as e:
            print(e)
        else:
            list = []
            for item in results_iterator:
                dict = {
                    'id': self._extract_property_id(item.name),
                    'name': item.display_name,
                    'time_zone': item.time_zone,
                    'currency': item.currency_code,
                    'industry': IndustryCategory(item.industry_category).name,
                    'service_level': ServiceLevel(item.service_level).name,
                    'create_time': datetime.fromtimestamp(
                        item.create_time.timestamp(),
                        pytz.timezone('Asia/Tokyo')
                    ),
                    'update_time': datetime.fromtimestamp(
                        item.update_time.timestamp(),
                        pytz.timezone('Asia/Tokyo')
                    )
                }
                list.append(dict)
            return list

    def create_custom_dimension(self, parameter_name, display_name, description, scope='EVENT'):
        """Create custom dimension for the property."""
        try:
            created_cd = self.client.create_custom_dimension(
                parent=f"properties/{self.property_id}",
                custom_dimension={
                    'parameter_name': parameter_name,
                    'display_name': display_name,
                    'description': description,
                    'scope': CustomDimension.DimensionScope[scope].value,
                }
            )
            return created_cd
        except Exception as e:
            print(e)

    def custom_dimensions(self, property_id):
        """Returns custom dimensions for the property."""
        try:
            results_iterator = self.client.list_custom_dimensions(
                parent=f"properties/{property_id}")
        except Exception as e:
            print(e)
        else:
            list = []
            for item in results_iterator:
                dict = {
                    'parameter_name': item.parameter_name,
                    'display_name': item.display_name,
                    'description': item.description,
                    'scope': CustomDimension.DimensionScope(item.scope).name,
                    #'disallow_ads_personalization': item.disallow_ads_personalization,
                }
                list.append(dict)
            return list

    def custom_metrics(self, property_id):
        """Returns custom metrics for the property."""
        try:
            results_iterator = self.client.list_custom_metrics(
                parent=f"properties/{property_id}")
        except Exception as e:
            print(e)
        else:
            list = []
            for item in results_iterator:
                dict = {
                    'parameter_name': item.parameter_name,
                    'display_name': item.display_name,
                    'description': item.description,
                    'scope': CustomDimension.DimensionScope(item.scope).name,
                    'measurement_unit': CustomMetric.MeasurementUnit(item.measurement_unit).name,
                    'restricted_metric_type': [CustomMetric.RestrictedMetricType(d).name for d in item.restricted_metric_type],
                }
                list.append(dict)
            return list

    def conf_data_retention(self, property_id):
        """Returns data retention settings for the property."""
        try:
            item = self.client.get_data_retention_settings(
                name=f"properties/{property_id}/dataRetentionSettings")
        except Exception as e:
            print(e)
        else:
                dict = {
                    'data_retention': DataRetentionSettings.RetentionDuration(item.event_data_retention).name,
                    'reset_user_data_on_new_activity': item.reset_user_data_on_new_activity,
                }
                return dict
