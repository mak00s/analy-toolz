"""
Functions for Google Analytics 4 API
"""

from datetime import datetime
import pandas as pd
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
from google.analytics.data_v1beta.types import FilterExpressionList
from google.analytics.data_v1beta.types import Metadata
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import MetricType
from google.analytics.data_v1beta.types import OrderBy
from google.analytics.data_v1beta.types import RunReportRequest
from google.oauth2.credentials import Credentials


class RoboGA4:
    required_scopes = [
        'https://www.googleapis.com/auth/analytics.edit',
        'https://www.googleapis.com/auth/analytics.readonly',
    ]

    def __init__(self, credentials, **kwargs):  # *args,
        """constructor"""
        self.credentials = credentials
        self.data_client = None
        self.admin_client = None
        self.account = self.Account(self)
        self.property = self.Property(self)
        self.report = self.Report(self)
        if credentials:
            self.authorize()

    def _parse_account_path(self, path: str):
        dict = self.admin_client.parse_account_path(path)
        return dict.get('account')

    def _parse_property_path(self, path: str):
        dict = self.admin_client.parse_property_path(path)
        return dict.get('property')

    def authorize(self):
        if isinstance(self.credentials, Credentials):
            print("Launching RoboGA4")
            self.data_client = BetaAnalyticsDataClient(credentials=self.credentials)
            self.admin_client = AnalyticsAdminServiceClient(credentials=self.credentials)
        else:
            print("credentials given are invalid.")
            return
        if bool(set(self.credentials.scopes) & set(self.required_scopes)):
            # print("scopes look good")
            pass
        else:
            print("the given scopes don't meet requirements.")
            return

    class Account:
        def __init__(self, parent):
            self.parent = parent
            self.id = None

        def select(self, id):
            self.id = id

        def list(self):
            """Returns summaries of all accounts accessible by the caller."""
            try:
                results_iterator = self.parent.admin_client.list_account_summaries()
            except Exception as e:
                print(e)
            else:
                list = []
                for item in results_iterator:
                    dict = {
                        'id': self.parent._parse_account_path(item.account),
                        'name': item.display_name,
                        'properties': [],
                    }
                    for p in item.property_summaries:
                        prop = {
                            'id': self.parent._parse_property_path(p.property),
                            'name': p.display_name
                        }
                        dict['properties'].append(prop)
                    list.append(dict)
                return list

    class Property:
        def __init__(self, parent):
            self.parent = parent
            self.id = None

        def select(self, id):
            self.id = id

        def info(self):
            properties = self.list()
            return [p for p in properties if p['id'] == self.id][0]

        def list(self):
            """Returns summaries of all properties for the account"""
            try:
                results_iterator = self.parent.admin_client.list_properties({
                    'filter': f"parent:accounts/{self.parent.account.id}",
                    'show_deleted': False,
                })
            except Exception as e:
                print(e)
            else:
                list = []
                for item in results_iterator:
                    dict = {
                        'id': self.parent._parse_property_path(item.name),
                        'name': item.display_name,
                        'time_zone': item.time_zone,
                        'currency': item.currency_code,
                        'industry': IndustryCategory(item.industry_category).name,
                        'service_level': ServiceLevel(item.service_level).name,
                        'created_time': datetime.fromtimestamp(
                            item.create_time.timestamp(),
                            pytz.timezone('Asia/Tokyo')
                        ),
                        'updated_time': datetime.fromtimestamp(
                            item.update_time.timestamp(),
                            pytz.timezone('Asia/Tokyo')
                        )
                    }
                    list.append(dict)
                return list

        def data_retention(self):
            """Returns data retention settings for the property."""
            try:
                item = self.parent.admin_client.get_data_retention_settings(
                    name=f"properties/{self.id}/dataRetentionSettings")
            except Exception as e:
                print(e)
            else:
                dict = {
                    'data_retention': DataRetentionSettings.RetentionDuration(item.event_data_retention).name,
                    'reset_user_data_on_new_activity': item.reset_user_data_on_new_activity,
                }
                return dict

        def available(self):
            """Returns available custom dimensions and custom metrics for the property."""
            path = self.parent.data_client.metadata_path(self.id)
            try:
                response = self.parent.data_client.get_metadata(name=path)
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
                return {'dimensions': dimensions, 'metrics': metrics}

        def list_custom_dimensions(self):
            """Returns custom dimensions for the property."""
            try:
                results_iterator = self.parent.admin_client.list_custom_dimensions(
                    parent=f"properties/{self.id}")
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
                        # 'disallow_ads_personalization': item.disallow_ads_personalization,
                    }
                    list.append(dict)
                return list

        def list_custom_metrics(self):
            """Returns custom metrics for the property."""
            try:
                results_iterator = self.parent.admin_client.list_custom_metrics(
                    parent=f"properties/{self.id}")
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
                        'restricted_metric_type': [CustomMetric.RestrictedMetricType(d).name for d in
                                                   item.restricted_metric_type],
                    }
                    list.append(dict)
                return list

        def create_custom_dimension(self, parameter_name, display_name, description, scope='EVENT'):
            """Create custom dimension for the property."""
            try:
                created_cd = self.parent.admin_client.create_custom_dimension(
                    parent=f"properties/{self.id}",
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

    class Report:
        def __init__(self, parent):
            self.parent = parent
            self.date_start = '7daysAgo'
            self.date_end = 'yesterday'

        def set_dates(self, date_start: str, date_end: str):
            self.date_start = date_start
            self.date_end = date_end

        def _ga4_response_to_dict(self, response):
            dim_len = len(response.dimension_headers)
            metric_len = len(response.metric_headers)
            all_data = []
            for row in response.rows:
                row_data = {}
                for i in range(0, dim_len):
                    row_data.update({response.dimension_headers[i].name: row.dimension_values[i].value})
                for i in range(0, metric_len):
                    row_data.update({response.metric_headers[i].name: row.metric_values[i].value})
                all_data.append(row_data)
            # df = pd.DataFrame(all_data)
            # return df
            return all_data

        def _convert_metric(self, value, type):
            """Metric's Value types are
                METRIC_TYPE_UNSPECIFIED = 0
                TYPE_CURRENCY = 9
                TYPE_FEET = 10
                TYPE_FLOAT = 2
                TYPE_HOURS = 7
                TYPE_INTEGER = 1
                TYPE_KILOMETERS = 13
                TYPE_METERS = 12
                TYPE_MILES = 11
                TYPE_MILLISECONDS = 5
                TYPE_MINUTES = 6
                TYPE_SECONDS = 4
                TYPE_STANDARD = 8
            """
            if type in ['TYPE_INTEGER', 'TYPE_HOURS','TYPE_MINUTES','TYPE_SECONDS','TYPE_MILLISECONDS']:
                return int(value)
            elif type in ['TYPE_FLOAT']:
                return float(value)
            else:
                return value

        def _parse_ga4_response(self, response):
            names = []
            dimension_types = []
            metrics_types = []
            for i in response.dimension_headers:
                names.append(i.name)
                dimension_types.append('category')
            for i in response.metric_headers:
                names.append(i.name)
                metrics_types.append(MetricType(i.type_).name)
            all_data = []
            for row in response.rows:
                row_data = []
                for d in row.dimension_values:
                    row_data.append(d.value)
                for i in range(0, len(row.metric_values)):
                    row_data.append(
                        self._convert_metric(
                            row.metric_values[i].value,
                            metrics_types[i]
                        )
                    )
                all_data.append(row_data)
            return all_data, names, dimension_types + metrics_types

        def _filter(self, exp, logic):
            if logic == 'AND':
                return FilterExpression(
                    and_group=FilterExpressionList(
                        expressions=[
                            FilterExpression(
                                filter=Filter(
                                    field_name="platform",
                                    string_filter=Filter.StringFilter(
                                        match_type=Filter.StringFilter.MatchType.EXACT,
                                        value="Android",
                                    ),
                                )
                            ),
                        ]
                    )
                )
            elif logic == 'OR':
                return FilterExpression(
                    or_group=FilterExpressionList(
                        expressions=[
                            FilterExpression(
                                filter=Filter()
                            ),
                        ]
                    )
                )
            elif logic == 'NOT':
                return FilterExpression(
                    not_expression=FilterExpression(
                        filter=Filter()
                    )
                )
            else:
                return FilterExpression(
                    filter=Filter()
                )

        def _call_api(
                self,
                dimensions: list,
                metrics: list,
                dimension_filter=None,
                metric_filter=None,
                order_bys=None,
                limit: int = 0,
                offset: int = 0,
        ):
            dimensions_ga4 = []
            for dimension in dimensions:
                dimensions_ga4.append(Dimension(name=dimension))
            metrics_ga4 = []
            for metric in metrics:
                metrics_ga4.append(Metric(name=metric))
            request = RunReportRequest(
                property=f"properties/{self.parent.property.id}",
                dimensions=dimensions_ga4,
                metrics=metrics_ga4,
                date_ranges=[DateRange(start_date=self.date_start, end_date=self.date_end)],
                dimension_filter=dimension_filter,
                metric_filter=metric_filter,
                order_bys=order_bys,
                offset=offset,
                limit=limit
            )
            response = self.parent.data_client.run_report(request)

            row_count = response.row_count
            data, headers, types = self._parse_ga4_response(response)

            return (data, row_count, headers, types)

        def run(
                self,
                dimensions: list,
                metrics: list,
                dimension_filter=None,
                metric_filter=None,
                order_bys=None,
                limit: int = 1000
        ):
            offset = 0
            all_rows = []
            page = 1

            while True:
                print(f"(p.{page})")
                (data, row_count, headers, types) = self._call_api(
                    dimensions,
                    metrics,
                    dimension_filter=dimension_filter,
                    metric_filter=metric_filter,
                    order_bys=order_bys,
                    limit=limit,
                    offset=offset
                )
                all_rows.extend(data)
                print(f"retrieved #{offset + 1} - #{offset + len(data)}")
                if offset + len(data) == row_count:
                    break
                else:
                    page += 1
                    offset += limit

            print(f"...retrieved all {row_count} rows.")

            return all_rows, headers, types, row_count

        def daily_pv(self):
            dimensions = [
                'date',
                'eventName',
            ]
            metrics = [
                'eventCount',
            ]
            dimension_filter = FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(value="page_view"),
                )
            )
            order_bys = [
                OrderBy(
                    desc=False,
                    dimension=OrderBy.DimensionOrderBy(
                        dimension_name="date"
                    )
                ),
                OrderBy(
                    desc=True,
                    metric=OrderBy.MetricOrderBy(
                        metric_name="eventCount"
                    )
                ),
            ]
            (data, headers, types, row_count) = self.run(
                dimensions,
                metrics,
                dimension_filter=dimension_filter,
                order_bys=order_bys
            )

            return headers, data

        def pv(self):
            dimensions = [
                # 'customUser:gtm_client_id',
                # 'customUser:ga_client_id',
                # 'customEvent:ga_session_number',
                # 'city',
                # 'customEvent:local_datetime',
                'eventName',
                'pagePath',
            ]
            metrics = [
                'eventCount',
                # 'customEvent:entrances',
                # 'customEvent:engagement_time_msec',
            ]
            (data, headers, types, row_count) = self.run(dimensions, metrics)

            return headers, data
