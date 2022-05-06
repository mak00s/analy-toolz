"""
Functions for Google Analytics 3 (Universal Analytics) API
"""

import pandas as pd
import re
import sys

from googleapiclient import errors
from google.oauth2.credentials import Credentials

from . import constants, ga4, google_api, utils


class Megaton(ga4.LaunchGA4):
    this = "Megaton UA"

    def __init__(self, credentials: Credentials, **kwargs):
        super().__init__(credentials, **kwargs)
        self.view = self.View(self)

    def build_client(self):
        self.data_client = google_api.GoogleApi(
            "analyticsreporting",
            "v4",
            constants.DEFAULT_SCOPES,
            credentials=self.credentials,
            credential_cache_file=self.credential_cache_file)
        self.admin_client = google_api.GoogleApi(
            "analytics",
            "v3",
            constants.DEFAULT_SCOPES,
            credentials=self.credentials,
            credential_cache_file=self.credential_cache_file)

    def update(self):
        """Returns account summaries accessible by the caller."""
        try:
            response = self.admin_client.management().accountSummaries().list().execute()
        except errors.HttpError as e:
            print("GCPのプロジェクトでGoogle Analytics APIを有効化してください。")
            return
        except Exception as e:
            type, value, traceback = sys.exc_info()
            print(f"type = {type}")
            print(f"value = {value}")
            raise e
        if response:
            results = []
            for i in response.get('items', []):
                account = {
                    'id': i['id'],
                    'name': i['name'],
                    'properties': [],
                }
                for p in i['webProperties']:
                    prop = {
                        'id': p['id'],
                        'name': p['name']
                    }
                    account['properties'].append(prop)
                results.append(account)
            self.accounts = results
            return results

    class Account(ga4.LaunchGA4.Account):
        def update(self):
            response = self.parent.admin_client.management().webproperties().list(
                accountId=self.id).execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'id': i['id'],
                    'name': i['name'],
                    'industry': i.get('industryVertical', ''),
                    'service_level': i['level'],
                    'created_time': i['created'],
                    'updated_time': i['updated'],
                    'data_retention': i['dataRetentionTtl'],
                    'data_retention_reset_on_activity': i['dataRetentionResetOnNewActivity'],
                    'properties': '',
                }
                results.append(dict)
            self.properties = results
            return results

    class Property(ga4.LaunchGA4.Property):
        def __init__(self, parent):
            super().__init__(parent)
            self.views = None

        def _get_metadata(self):
            return {'dimensions': [], 'metrics': []}

        def _get_custom_dimensions(self):
            """Returns custom dimensions for the property."""
            response = self.parent.admin_client.management().customDimensions().list(
                accountId=self.parent.account.id,
                webPropertyId=self.parent.property.id
            ).execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'index': i['index'],
                    'display_name': i['name'],
                    'scope': i['scope'],
                    'active': i['active'],
                }
                results.append(dict)
            return results

        def _get_custom_metrics(self):
            """Returns custom metrics for the property."""
            response = self.parent.admin_client.management().customMetrics().list(
                accountId=self.parent.account.id,
                webPropertyId=self.parent.property.id
            ).execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'index': i['index'],
                    'display_name': i['name'],
                    'scope': i['scope'],
                    'type': i['type'],
                    'active': i['active'],
                }
                results.append(dict)
            return results

        def get_dimensions(self):
            if not self.api_custom_dimensions:
                self.api_custom_dimensions = self._get_custom_dimensions()
            return self.api_custom_dimensions

        def get_metrics(self):
            if not self.api_custom_metrics:
                self.api_custom_metrics = self._get_custom_metrics()
            return self.api_custom_metrics

        def clear(self):
            super().clear()
            self.views = None

        def update(self):
            self.clear()
            self.get_info()
            # self.get_available()

            # get views
            response = self.parent.admin_client.management().profiles().list(
                accountId=self.parent.account.id,
                webPropertyId=self.parent.property.id
            ).execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'property_id': i['webPropertyId'],
                    'id': i['id'],
                    'name': i['name'],
                    'currency': i['currency'],
                    'time_zone': i['timezone'],
                    'url': i['websiteUrl'],
                    'type': i['type'],
                    'ecommerce': i['eCommerceTracking'],
                    'bot_filtering': i.get('botFilteringEnabled', False),
                    'site_search_parameters': i.get('siteSearchQueryParameters', ''),
                    'default_page': i.get('defaultPage', ''),
                    'created_time': i['created'],
                    'updated_time': i['updated'],
                }
                results.append(dict)
            self.views = results
            return results

        def show(
                self,
                me: str = 'info',
                index_col: str = None
        ):
            res = None
            sort_values = []
            if me == 'custom_dimensions':
                index_col = 'index'
                res = self.get_dimensions()
            elif me == 'custom_metrics':
                index_col = 'index'
                res = self.get_metrics()

            if res:
                if index_col:
                    return pd.DataFrame(res).set_index(index_col).sort_values(by=sort_values)
                else:
                    return pd.DataFrame(res).sort_values(by=sort_values)
            return pd.DataFrame()

    class View:
        def __init__(self, parent):
            self.parent = parent
            self.id = None
            self.name = None
            self.currency = None
            self.time_zone = None
            self.url = None
            self.type = None
            self.ecommerce = None
            self.bot_filtering = None
            self.site_search_parameters = None
            self.default_page = None
            self.created_time = None
            self.updated_time = None

        def clear(self):
            self.name = None
            self.currency = None
            self.time_zone = None
            self.url = None
            self.type = None
            self.ecommerce = None
            self.bot_filtering = None
            self.site_search_parameters = None
            self.default_page = None
            self.created_time = None
            self.updated_time = None

        def select(self, id: str):
            if id:
                if id != self.id:
                    self.id = id
                    self.update()
            else:
                self.id = None
                self.clear()

        def update(self):
            self.get_info()

        def get_info(self):
            """Get view data from parent property"""
            i = [p for p in self.parent.property.views if p['id'] == self.id][0]
            # print(dict)
            self.name = i['name']
            self.currency = i['currency']
            self.time_zone = i['time_zone']
            self.url = i['url']
            self.type = i['type']
            self.ecommerce = i['ecommerce']
            self.bot_filtering = i['bot_filtering']
            self.site_search_parameters = i['site_search_parameters']
            self.default_page = i['default_page']
            self.created_time = i['created_time']
            self.updated_time = i['updated_time']
            return i

        def show(
                self,
                me: str = 'info',
                index_col: str = None
        ):
            res = None
            sort_values = []
            if me == 'info':
                res = [self.get_info()]
                index_col = 'id'

            if res:
                if index_col:
                    return pd.DataFrame(res).set_index(index_col).sort_values(by=sort_values)
                else:
                    return pd.DataFrame(res).sort_values(by=sort_values)
            return pd.DataFrame()

    class Report(ga4.LaunchGA4.Report):

        def _format_api_name(self, before, which='dimension'):
            if which == 'dimension':
                return {'name': f"ga:{before}"}
            else:
                return {'expression': f"ga:{before}", 'alias': before}

        def _parse_filter(self, before: str):
            m = re.search(r'^(.+)(==|!=|=@|!@|=~|!~|>|<)(.+)$', before)
            if m:
                field = m.groups()[0]
                value = m.groups()[2]
                op = m.groups()[1]
                is_not = True if op.startswith('!') else False
                operator = 'PARTIAL'
                if op.endswith('='):
                    operator = 'EXACT'
                elif op.endswith('~'):
                    operator = 'REGEXP'
                elif op == '>':
                    operator = 'GREATER_THAN'
                elif op == '<':
                    operator = 'LESS_THAN'
                return is_not, operator, field, value

        def _format_dimension_filter(self, before: str):
            if not before:
                return []
            clause = {'operator': 'AND', 'filters': []}
            for i in before.split(';'):
                is_not, operator, field, value = self._parse_filter(i)
                filter = {
                    'dimensionName': field,
                    'not': is_not,
                    'operator': operator,
                    'expressions': [value],
                    'caseSensitive': False
                }
                clause['filters'].append(filter)
            return [clause]

        def _format_metric_filter(self, before: str):
            if not before:
                return []
            clause = {'operator': 'AND', 'filters': []}
            for i in before.split(';'):
                is_not, operator, field, value = self._parse_filter(i)
                filter = {
                    'metricName': field,
                    'not': is_not,
                    'operator': operator,
                    'comparisonValue': value
                }
                clause['filters'].append(filter)
            return [clause]

        def _format_order_bys(self, before: str):
            result = []
            for i in before.split(','):
                obj = {}
                try:
                    _, field = i.split('-')
                except ValueError:
                    obj['fieldName'] = i
                    obj['sortOrder'] = 'ASCENDING'
                else:
                    obj['fieldName'] = field
                    obj['sortOrder'] = 'DESCENDING'
                result.append(obj)
            return result

        def _format_report_request(self, **kwargs):
            """Construct a request for API"""
            dimension_api_names = [self._format_api_name(r) for r in kwargs.get('dimensions')]
            metrics_api_names = [self._format_api_name(r, which='metrics') for r in kwargs.get('metrics')]
            return {
                'viewId': self.parent.view.id,
                'dateRanges': [{
                    'startDate': kwargs.get('start_date'),
                    'endDate': kwargs.get('end_date')
                }],
                'samplingLevel': 'LARGE',
                'dimensions': dimension_api_names,
                'dimensionFilterClauses': self._format_dimension_filter(kwargs.get('dimension_filter')),
                'metrics': metrics_api_names,
                'metricFilterClauses': self._format_metric_filter(kwargs.get('metric_filter')),
                'orderBys': self._format_order_bys(kwargs.get('order_bys')),
                'pageSize': kwargs.get('limit'),
                'includeEmptyRows': True,
                'hideTotals': not kwargs.get('show_total', False),
                'hideValueRanges': True,
            }

        def _call_report_api(self, limit: int, offset: int, request: dict):

            request["pageSize"] = limit
            if offset:
                request["pageToken"] = offset

            response = self.parent.data_client.reports().batchGet(
                body={
                    'reportRequests': [request],
                    'useResourceQuotas': False  # only for 360
                }
            ).execute()

            report_data = response.get('reports', [])[0]

            total_rows = report_data['data']['rowCount']

            samples_count = report_data['data'].get('samplesReadCounts')
            samples_size = report_data['data'].get('samplingSpaceSizes')
            next_token = report_data.get('nextPageToken', None)
            if samples_count:
                print(f"samplesReadCounts = {samples_count}")
            if samples_size:
                print(f"samplingSpaceSizes = {samples_size}")

            data, headers, types = self._parse_response(report_data)

            return data, total_rows, headers, types, next_token

        def _parse_response(self, report: dict):
            all_data = []
            names = []
            dimension_types = []
            metric_types = []

            for i in report['columnHeader']['dimensions']:
                names.append(i.replace('ga:', ''))
                dimension_types.append('category')

            for i in report['columnHeader']['metricHeader']['metricHeaderEntries']:
                names.append(i['name'])
                metric_types.append(i['type'])

            for row in report['data']['rows']:
                row_data = []
                for d in row['dimensions']:
                    row_data.append(d)
                for d in row['metrics']:
                    for i, v in enumerate(d['values']):
                        row_data.append(self._convert_metric(v, metric_types[i]))
                all_data.append(row_data)

            return all_data, names, dimension_types + metric_types

        def _generator(
                self,
                dimensions: list,
                metrics: list,
                start_date: str,
                end_date: str,
                dimension_filter=None,
                metric_filter=None,
                order=None,
                show_total: bool = False,
                limit: int = 10000,
        ):
            """Send request to get report data"""
            if not self.parent.view.id:
                # print("Viewを先に選択してから実行してください。")
                return

            request = self._format_report_request(
                start_date=start_date,
                end_date=end_date,
                dimensions=dimensions,
                metrics=metrics,
                dimension_filter=dimension_filter,
                metric_filter=metric_filter,
                order_bys=order,
                show_total=show_total,
            )
            # print(request)

            offset = 0
            while True:
                try:
                    (data, total_rows, headers, types, next_token) = self._call_report_api(limit, offset, request)
                except errors.HttpError as e:
                    value = str(sys.exc_info()[1])
                    if 'disabled' in value:
                        print("\nGCPのプロジェクトでAnalytics Reporting APIを有効化してください。")
                        return
                    elif 'Invalid value' in value:
                        print("レポート抽出条件の書式や内容を見直してください。")
                        return
                    else:
                        raise e

                if offset == 0 and total_rows:
                    print(f"Total {total_rows} rows returned.")
                    self.headers = headers
                    self.types = types

                if len(data):
                    print(f"(retrieved row #{offset}-{int(offset) + len(data) - 1})")

                for row in data:
                    yield row

                if not next_token:
                    break
                offset = next_token

        def show(
                self,
                dimensions: list,
                metrics: list,
                start_date=None,
                end_date=None,
                dimension_filter=None,
                metric_filter=None,
                order=None,
                show_total: bool = False,
                limit: int = 10000,
                to_pd: bool = True,
        ):
            """Send request to get report data"""
            if not self.parent.view.id:
                print("Viewを先に選択してから実行してください。")
                return

            start_date = start_date if start_date else self.start_date
            end_date = end_date if end_date else self.end_date
            print(f"Requesting a report ({start_date} - {end_date})")

            gen = self._generator(
                dimensions=dimensions,
                metrics=metrics,
                start_date=start_date,
                end_date=end_date,
                dimension_filter=dimension_filter,
                metric_filter=metric_filter,
                order=order,
                limit=limit
            )

            if to_pd:
                df = pd.DataFrame(list(gen), columns=self.headers)
                return df
            else:
                return list(gen)

        def _old_run(
                self,
                dimensions: list,
                metrics: list,
                start_date=None,
                end_date=None,
                dimension_filter=None,
                metric_filter=None,
                order=None,
                show_total: bool = False,
                limit: int = 10000,
                to_pd: bool = False,
        ):
            """Send request to get report data"""
            if not self.parent.view.id:
                print("Viewを先に選択してから実行してください。")
                return

            start_date = start_date if start_date else self.start_date
            end_date = end_date if end_date else self.end_date
            print(f"Requesting a report ({start_date} - {end_date})")

            request = self._format_report_request(
                start_date=start_date,
                end_date=end_date,
                dimensions=dimensions,
                metrics=metrics,
                dimension_filter=dimension_filter,
                metric_filter=metric_filter,
                order_bys=order,
                show_total=show_total,
            )
            print(request)

            # all_rows = []
            offset = 0
            while True:
                try:
                    (data, total_rows, headers, types, next_token) = self._call_report_api(limit, offset, request)
                except errors.HttpError as e:
                    value = str(sys.exc_info()[1])
                    if 'disabled' in value:
                        print("\nGCPのプロジェクトでAnalytics Reporting APIを有効化してください。")
                        return
                    elif 'Invalid value' in value:
                        print("レポート抽出条件の書式や内容を見直してください。")
                        return
                    else:
                        raise e

                if offset == 0 and total_rows:
                    print(f"Total {total_rows} rows returned.")
                    self.headers = headers
                    self.types = types

                if len(data):
                    print(f"(retrieved row #{offset}-{int(offset) + len(data) - 1})")
                    # all_rows.extend(data)

                for row in data:
                    yield row

                if not next_token:
                    break
                offset = next_token

            # if to_pd:
            #     df = pd.DataFrame(all_rows, columns=headers)
            #     df.columns = dimensions + metrics
            #     return df
            # else:
            #     return all_rows, headers, types
