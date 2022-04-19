"""
Functions for Google Analytics 4 API
"""

from collections import OrderedDict
from datetime import datetime
import pandas as pd
import pytz
import re
import sys

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
from google.analytics.data_v1beta.types import MetricAggregation
from google.analytics.data_v1beta.types import MetricType
from google.analytics.data_v1beta.types import OrderBy
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta.types import RunReportResponse
from google.api_core.exceptions import PermissionDenied
from google.oauth2.credentials import Credentials

from . import utils


class GA4:
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
        self.accounts = None
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
            print("Launching megaton GA4")
            self.data_client = BetaAnalyticsDataClient(credentials=self.credentials)
            self.admin_client = AnalyticsAdminServiceClient(credentials=self.credentials)
            self.update()
        else:
            print("credentials given are invalid.")
            return
        if bool(set(self.credentials.scopes) & set(self.required_scopes)):
            # print("scopes look good")
            pass
        else:
            print("the given scopes don't meet requirements.")
            return

    def update(self):
        """Returns summaries of all accounts accessible by the caller."""
        try:
            results_iterator = self.admin_client.list_account_summaries()
        except PermissionDenied as e:
            print("権限がありません。")
            m = re.search(r'reason: "([^"]+)', str(sys.exc_info()[1]))
            if m:
                reason = m.group(1)
                if reason == 'SERVICE_DISABLED':
                    print("GCPのプロジェクトでAdmin APIを有効化してください。")
            message = getattr(e, 'message', repr(e))
            print(message)
        except Exception as e:
            type, value, traceback = sys.exc_info()
            print(type)
            print(value)
        else:
            results = []
            for i in results_iterator:
                dict1 = {
                    'id': self._parse_account_path(i.account),
                    'name': i.display_name,
                    'properties': [],
                }
                for p in i.property_summaries:
                    dict2 = {
                        'id': self._parse_property_path(p.property),
                        'name': p.display_name
                    }
                    dict1['properties'].append(dict2)
                results.append(dict1)
            self.accounts = results
            return results

    class Account:
        def __init__(self, parent):
            self.parent = parent
            self.id = None
            self.properties = None

        def select(self, id: str):
            if id:
                if id != self.id:
                    self.id = id
                    self.update()
            else:
                self.parent.property.id = None

        def update(self):
            """Update summaries of all properties for the account"""
            try:
                results_iterator = self.parent.admin_client.list_properties({
                    'filter': f"parent:accounts/{self.id}",
                    'show_deleted': False,
                })
            except Exception as e:
                # print(e)
                raise e
            else:
                results = []
                for i in results_iterator:
                    dict = {
                        'id': self.parent._parse_property_path(i.name),
                        'name': i.display_name,
                        'time_zone': i.time_zone,
                        'currency': i.currency_code,
                        'industry': IndustryCategory(i.industry_category).name,
                        'service_level': ServiceLevel(i.service_level).name,
                        'created_time': datetime.fromtimestamp(
                            i.create_time.timestamp(),
                            pytz.timezone('Asia/Tokyo')
                        ),
                        'updated_time': datetime.fromtimestamp(
                            i.update_time.timestamp(),
                            pytz.timezone('Asia/Tokyo')
                        )
                    }
                    results.append(dict)
                self.properties = results
                return results

        def show(
                self,
                # me: str = 'properties',
                index_col: str = 'id'
        ):
            res = self.properties
            if res:
                df = pd.DataFrame(res)
                if index_col:
                    return df.set_index(index_col)

    class Property:
        def __init__(self, parent):
            self.parent = parent
            self.id = None
            self.name = None
            self.created_time = None
            self.updated_time = None
            self.time_zone = None
            self.currency = None
            self.industry = None
            self.service_level = None
            self.data_retention = None
            self.data_retention_reset_on_activity = None
            self.api_custom_dimensions = None
            self.api_custom_metrics = None
            self.api_metadata = None
            self.dimensions = None
            self.metrics = None
            # self.clear()

        def _get_metadata(self):
            """Returns available custom dimensions and custom metrics for the property."""
            path = self.parent.data_client.metadata_path(self.id)
            try:
                response = self.parent.data_client.get_metadata(name=path)
            except Exception as e:
                print(e)
                raise e
            else:
                dimensions = []
                for i in response.dimensions:
                    dimensions.append({
                        'customized':   i.custom_definition,
                        'category':     i.category,
                        'api_name':     i.api_name,
                        'display_name': i.ui_name,
                        'description':  i.description,
                        # 'deprecated_api_names': i.deprecated_api_names,
                    })
                metrics = []
                for i in response.metrics:
                    metrics.append({
                        'customized':   i.custom_definition,
                        'category':     i.category,
                        'api_name':     i.api_name,
                        'display_name': i.ui_name,
                        'description':  i.description,
                        # 'deprecated_api_names': i.deprecated_api_names,
                        'type':         i.type_,
                        'expression':   i.expression,
                    })
                return {'dimensions': dimensions, 'metrics': metrics}

        def _get_custom_dimensions(self):
            """Returns custom dimensions for the property."""
            try:
                results_iterator = self.parent.admin_client.list_custom_dimensions(
                    parent=f"properties/{self.id}")
            except Exception as e:
                print(e)
            else:
                results = []
                for i in results_iterator:
                    dict = {
                        'parameter_name': i.parameter_name,
                        'display_name': i.display_name,
                        'description': i.description,
                        'scope': CustomDimension.DimensionScope(i.scope).name,
                        # 'disallow_ads_personalization': item.disallow_ads_personalization,
                    }
                    results.append(dict)
                return results

        def _get_custom_metrics(self):
            """Returns custom metrics for the property."""
            try:
                results_iterator = self.parent.admin_client.list_custom_metrics(
                    parent=f"properties/{self.id}")
            except Exception as e:
                print(e)
            else:
                results = []
                for i in results_iterator:
                    dict = {
                        'parameter_name': i.parameter_name,
                        'display_name': i.display_name,
                        'description': i.description,
                        'scope': CustomDimension.DimensionScope(i.scope).name,
                        'measurement_unit': CustomMetric.MeasurementUnit(i.measurement_unit).name,
                        'restricted_metric_type': [CustomMetric.RestrictedMetricType(d).name for d in
                                                   i.restricted_metric_type],
                    }
                    results.append(dict)
                return results

        def _get_data_retention(self):
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

        def select(self, id: str):
            if id:
                if id != self.id:
                    self.id = id
                    self.update()
            else:
                self.id = None
                self.clear()

        def clear(self):
            self.name = None
            self.created_time = None
            self.updated_time = None
            self.time_zone = None
            self.currency = None
            self.industry = None
            self.service_level = None
            self.data_retention = None
            self.data_retention_reset_on_activity = None
            self.api_custom_dimensions = None
            self.api_custom_metrics = None
            self.api_metadata = None
            self.dimensions = None
            self.metrics = None

        def update(self):
            self.clear()
            self.get_info()
            self.get_available()

        def get_info(self):
            """Get property data from parent account"""
            dict = [p for p in self.parent.account.properties if p['id'] == self.id][0]
            self.name = dict['name']
            self.created_time = dict['created_time']
            self.updated_time = dict['updated_time']
            self.time_zone = dict['time_zone']
            self.currency = dict['currency']
            self.industry = dict['industry']
            self.service_level = dict['service_level']
            if not self.data_retention:
                dict2 = self._get_data_retention()
                dict['data_retention'] = dict2['data_retention']
                dict['data_retention_reset_on_activity'] = dict2['reset_user_data_on_new_activity']

            return dict

        def get_available(self):
            if not self.api_metadata:
                self.api_metadata  = self._get_metadata()
            return self.api_metadata

        def get_dimensions(self):
            self.get_available()
            if not self.api_custom_dimensions:
                self.api_custom_dimensions = self._get_custom_dimensions()
            # integrate data
            new = []
            for m in self.api_metadata['dimensions']:
                dict = m.copy()
                if m['customized'] == True:
                    for c in self.api_custom_dimensions:
                        if m['display_name'] == c['display_name'] or m['display_name'] == c['parameter_name']:
                            dict['description'] = c['description']
                            dict['parameter_name'] = c['parameter_name']
                            dict['scope'] = c['scope']
                new.append(dict)
            self.dimensions = new
            return self.dimensions

        def get_metrics(self):
            self.get_available()
            if not self.api_custom_metrics:
                self.api_custom_metrics = self._get_custom_metrics()
            # integrate data
            new = []
            for m in self.api_metadata['metrics']:
                dict = m.copy()
                if m['customized'] == True:
                    for c in self.api_custom_metrics or {}:
                        if m['display_name'] == c['display_name']:
                            dict['description'] = c['description']
                            dict['parameter_name'] = c['parameter_name']
                            dict['scope'] = c['scope']
                            dict['unit'] = c['measurement_unit']
                if 'type' in m.keys():
                    dict['type'] = MetricType(m['type']).name
                new.append(dict)
            self.metrics = new
            return self.metrics

        def show(
                self,
                me: str = 'info',
                index_col: str = None #'parameter_name'
        ):
            res = None
            if me == 'metrics':
                list_of_dict = self.get_metrics()
                my_order = ["category", "display_name", "description", "api_name", "parameter_name", "scope", "unit", "expression"]
                res = []
                for d in list_of_dict:
                    res.append(OrderedDict((k, d[k]) for k in my_order if k in d.keys()))
                sort_values = ['customized', 'category', 'display_name']
            elif me == 'dimensions':
                list_of_dict = self.get_dimensions()
                my_order = ["category", "display_name", "description", "api_name", "parameter_name", "scope"]
                res = []
                for d in list_of_dict:
                    res.append(OrderedDict((k, d[k]) for k in my_order if k in d.keys()))
                sort_values = ['customized', 'category', 'display_name']
            elif me == 'custom_dimensions':
                index_col = 'api_name'
                dict = self.get_dimensions()
                res = []
                for r in dict:
                    if r['customized']:
                        res.append({
                            'display_name': r['display_name'],
                            'api_name': r['api_name'],
                            'parameter_name': r['parameter_name'],
                            'description': r['description'],
                            'scope': r['scope'],
                        })

            elif me == 'custom_metrics':
                index_col = 'api_name'
                dict = self.get_metrics()
                res = []
                for r in dict:
                    if r['customized']:
                        res.append({
                            'display_name': r['display_name'],
                            'api_name': r['api_name'],
                            'description': r['description'],
                            'type': r['type'] if 'type' in r else '',
                            'scope': r['scope'] if 'scope' in r else '',
                            'parameter_name': r['parameter_name'] if 'parameter_name' in r else '',
                            'unit': r['unit'] if 'unit' in r else '',
                            'expression': r['expression'],
                        })
            elif me == 'info':
                res = [self.get_info()]
                index_col = 'id'

            if res:
                if index_col:
                    return pd.DataFrame(res).set_index(index_col)
                else:
                    return pd.DataFrame(res)
            return pd.DataFrame().sort_values(by=sort_values)

        def create_custom_dimension(
                self,
                parameter_name: str,
                display_name: str,
                description: str,
                scope: str = 'EVENT'
        ):
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
            self.start_date = '7daysAgo'
            self.end_date = 'yesterday'

        def set_dates(self, start_date: str, end_date: str):
            self.start_date = start_date
            self.end_date = end_date

        def _get_api_name(self, before, which='dimensions', what='api_name'):
            dim = self.parent.property.api_metadata[which]
            for r in dim:
                if r['display_name'] == before:
                    return r[what]
                elif r['api_name'] == before:
                    return r[what]
            print(f"{which[:-1]} {before} is not found.")
            return None

        # def _get_dimension_api_name(self, dim: str):
        #     name = [
        #         'custom'
        #         + r['scope'][0:].title()
        #         + ':'
        #         + r['parameter_name']
        #         for r in self.parent.property.dimensions if r['parameter_name'] == dim][0]
        #     return name

        def _ga4_response_to_dict(self, response: RunReportResponse):
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
            return all_data

        def _convert_metric(self, value, type: str):
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

        def _parse_ga4_response(self, response: RunReportResponse):
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

        def _format_filter(self, conditions, logic=None):
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
            return

        def _call_api(
                self,
                dimensions: list,
                metrics: list,
                start_date=None,
                end_date=None,
                dimension_filter=None,
                metric_filter=None,
                order_bys=None,
                show_total: bool = False,
                limit: int = 0,
                offset: int = 0,
        ):
            dimensions_ga4 = [Dimension(name=d) for d in dimensions]
            # for d in dimensions:
            #     dimensions_ga4.append(Dimension(name=d))

            metrics_ga4 = [Metric(name=m) for m in metrics]
            # for m in metrics:
            #     metrics_ga4.append(Metric(name=m))

            date_ranges = [DateRange(start_date=start_date, end_date=end_date)]

            metric_aggregations = []
            if show_total:
                metric_aggregations = [
                    MetricAggregation.TOTAL,
                    MetricAggregation.MAXIMUM,
                    MetricAggregation.MINIMUM,
                ]

            request = RunReportRequest(
                property=f"properties/{self.parent.property.id}",
                dimensions=dimensions_ga4,
                metrics=metrics_ga4,
                date_ranges=date_ranges,
                dimension_filter=dimension_filter,
                metric_filter=metric_filter,
                order_bys=order_bys,
                limit=limit,
                offset=offset,
            )

            data = []
            headers = []
            types = []
            row_count = 0
            response = None
            try:
                response = self.parent.data_client.run_report(request)
                row_count = response.row_count
            except PermissionDenied as e:
                print("権限がありません。")
                message = getattr(e, 'message', repr(e))
                ex_value = sys.exc_info()[1]
                m = re.search(r'reason: "([^"]+)', str(ex_value))
                if m:
                    reason = m.group(1)
                if reason == 'SERVICE_DISABLED':
                    print("GCPのプロジェクトでData APIを有効化してください。")
                print(message)
            except Exception as e:
                # print(e)
                type_, value, traceback_ = sys.exc_info()
                print(type_)
                print(value)

            if row_count > 0:
                data, headers, types = self._parse_ga4_response(response)

            return data, row_count, headers, types

        def run(
                self,
                dimensions: list,
                metrics: list,
                start_date=None,
                end_date=None,
                dimension_filter=None,
                metric_filter=None,
                order_bys=None,
                show_total: bool = False,
                limit: int = 10000,
                to_pd: bool = True
        ):
            offset = 0
            all_rows = []
            headers = []
            types = []
            page = 1

            dimension_api_names = [self._get_api_name(r) for r in dimensions]
            metrics_api_names = [self._get_api_name(r, which='metrics') for r in metrics]
            start_date = start_date if start_date else self.start_date
            end_date = end_date if end_date else self.end_date
            print(f"Building a report ({start_date} - {end_date})")

            while True:
                (data, row_count, headers, types) = self._call_api(
                    dimension_api_names,
                    metrics_api_names,
                    start_date=start_date,
                    end_date=end_date,
                    dimension_filter=dimension_filter,
                    metric_filter=metric_filter,
                    order_bys=order_bys,
                    show_total=show_total,
                    limit=limit,
                    offset=offset
                )
                if len(data) > 0:
                    all_rows.extend(data)
                    if offset == 0:
                        print(f"Total {row_count} rows found.")
                    print(f" p{page}: retrieved #{offset + 1} - #{offset + len(data)}")
                    if offset + len(data) == row_count:
                        break
                    else:
                        page += 1
                        offset += limit
                else:
                    break

            if len(all_rows) > 0:
                print(f"All {len(all_rows)} rows were retrieved.")
                if to_pd:
                    df = pd.DataFrame(all_rows, columns=headers)
                    df = utils.change_column_type(df)
                    df.columns = dimensions + metrics
                    return df
                else:
                    return all_rows, headers, types
            else:
                print("no data found.")
                if to_pd:
                    return pd.DataFrame()
                else:
                    return all_rows, headers, types

        """
        reports
        """
        def audit(self, dimension: str = 'eventName', metric: str = 'eventCount'):
            """Audit collected data for a dimension or a metric specified
            Args:
                dimension (str): api_name or display_name of a dimension
            """
            df_e = self.run(
                [dimension, 'date'],
                [metric],
                start_date=self.parent.property.created_time.strftime("%Y-%m-%d"),
                end_date='yesterday'
            )
            if len(df_e) > 0:
                return df_e.groupby(dimension).sum().merge(
                    df_e.groupby(dimension).agg({'date': 'min'}), on=dimension, how='left').merge(
                    df_e.groupby(dimension).agg({'date': 'max'}), on=dimension, how='left',
                    suffixes=['_first', '_last']).sort_values(by=[metric], ascending=False)
            else:
                return pd.DataFrame()

        def audit_dimensions(self, only: list = [], ignore: list = []):
            """ディメンションの計測アイテム毎の回数・記録された最初と最後の日
            """
            if not only:
                only = self.parent.property.show('custom_dimensions').index.to_list()

            dict = {}
            for item in only:
                if item not in ignore:
                    print(f"Auditing dimension {item}...")
                    dict[item] = self.audit(item)
                    print()
            print("...done")
            return dict

        def audit_metrics(self, only: list = [], ignore: list = []):
            if not only:
                only = [d['api_name'] for d in self.parent.property.metrics if 'scope' in d]

            dict = {}
            for item in only:
                if item not in ignore:
                    print(f"Auditing metric {item}...")
                    dict[item] = self.audit(metric=item)
                    print()
            print("...done")
            return dict

        def pv_by_day(self):
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
            ]
            return self.run(
                dimensions,
                metrics,
                dimension_filter=dimension_filter,
                order_bys=order_bys
            )

        def events_by_day(self):
            dimensions = [
                'date',
                'eventName',
            ]
            metrics = [
                'eventCount',
            ]
            # dimension_filter = FilterExpression(
            #     filter=Filter(
            #         field_name="eventName",
            #         string_filter=Filter.StringFilter(value="page_view"),
            #     )
            # )
            # dimension_filter = FilterExpression(
            #     not_expression=FilterExpression(
            #     filter=Filter(
            #         field_name="eventName",
            #         numeric_filter=Filter.NumericFilter(
            #             operation=Filter.NumericFilter.Operation.GREATER_THAN,
            #             value=NumericValue(int64_value=1000),
            #         ),
            #     )
            #     )
            # )
            # dimension_filter = FilterExpression(
            #     filter=Filter(
            #         field_name="eventName",
            #         in_list_filter=Filter.InListFilter(
            #             values=[
            #                 "purchase",
            #                 "in_app_purchase",
            #                 "app_store_subscription_renew",
            #             ]
            #         ),
            #     )
            # )
            # dimension_filter = FilterExpression(
            #     and_group=FilterExpressionList(
            #         expressions=[
            #             FilterExpression(
            #                 filter=Filter(
            #                     field_name="browser",
            #                     string_filter=Filter.StringFilter(value="Chrome"),
            #                 )
            #             ),
            #             FilterExpression(
            #                 filter=Filter(
            #                     field_name="countryId",
            #                     string_filter=Filter.StringFilter(value="US"),
            #                 )
            #             ),
            #         ]
            #     )
            # )
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
            return self.run(
                dimensions,
                metrics,
                # dimension_filter=dimension_filter,
                order_bys=order_bys
            )

        def custom_dimensions(self):
            dimensions = [
                'date',
                'eventName',
            ]
            metrics = [
                'eventCount',
            ]
            # dimension_filter = FilterExpression(
            #     filter=Filter(
            #         field_name="eventName",
            #         string_filter=Filter.StringFilter(value="page_view"),
            #     )
            # )
            order_bys = [
                OrderBy(
                    desc=False,
                    dimension=OrderBy.DimensionOrderBy(
                        dimension_name="date"
                    )
                ),
            ]
            return self.run(
                dimensions,
                metrics,
                # dimension_filter=dimension_filter,
                order_bys=order_bys
            )

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
            (data, headers, types) = self.run(dimensions, metrics)

            return headers, data
