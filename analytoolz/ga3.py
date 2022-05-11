"""
Functions for Google Analytics 3 (Universal Analytics) API
"""

from typing import Optional
import json
import logging
import pandas as pd
import re
import sys

from googleapiclient import errors as err
from google.oauth2.credentials import Credentials

from . import constants, errors, ga4, google_api, utils

LOGGER = logging.getLogger(__name__)


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

    def _update(self):
        """Returns account summaries accessible by the caller."""
        try:
            response = self.admin_client.management().accountSummaries().list().execute()
        except err.HttpError as e:
            if e.resp.status == 403:
                LOGGER.error(f"GCPのプロジェクトでGoogle Analytics APIを有効化してください。{e.resp.status}")
                raise errors.ApiDisabled
        except Exception as e:
            type, value, _ = sys.exc_info()
            LOGGER.debug(f"type = {type}")
            LOGGER.debug(f"value = {value}")
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
        def _update(self):
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

        def _clear(self):
            super()._clear()
            self.views = None

        def _update(self):
            self._clear()
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

        def show(self, me: str = 'info', index_col: Optional[str] = None):
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

        def show(self, me: str = 'info', index_col: Optional[str] = None):
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

        def _format_name(self, before: str, as_: str = None):
            after = ("ga:" + before) if not before.startswith("ga:") else before
            if as_ == 'dimension':
                return {'name': after}
            elif as_ == 'metric':
                return {'expression': after, 'alias': before}
            else:
                return after

        def _parse_filter(self, before: str):
            m = re.search(r'^(.+)(==|!=|=@|!@|=~|!~|>|<)(.+)$', before)
            if m:
                field = self._format_name(m.groups()[0])
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
                try:
                    is_not, operator, field, value = self._parse_filter(i)
                    filter = {
                        'metricName': field,
                        'not': is_not,
                        'operator': operator,
                        'comparisonValue': value
                    }
                    clause['filters'].append(filter)
                except TypeError:
                    LOGGER.warn("metric filter is invalid. ignoring...")
            return [clause]

        def _format_order_bys(self, before: str):
            if not before:
                return
            result = []
            for i in before.split(','):
                obj = {}
                try:
                    _, field = i.split('-')
                except ValueError:
                    obj['fieldName'] = self._format_name(i)
                    obj['sortOrder'] = 'ASCENDING'
                else:
                    obj['fieldName'] = self._format_name(field)
                    obj['sortOrder'] = 'DESCENDING'
                result.append(obj)
            return result

        def _format_request(self, **kwargs):
            """Construct a request for API"""
            dimension_api_names = [self._format_name(r, as_='dimension') for r in kwargs.get('dimensions')]
            metrics_api_names = [self._format_name(r, as_='metric') for r in kwargs.get('metrics')]
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
                'segments': kwargs.get('segments'),
                'includeEmptyRows': False,
                'hideTotals': not kwargs.get('show_total', False),
                'hideValueRanges': True,
                'pageSize': kwargs.get('limit'),
            }

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

            for row in report['data'].get('rows', []):
                row_data = []
                for d in row['dimensions']:
                    row_data.append(d)
                for d in row['metrics']:
                    for i, v in enumerate(d['values']):
                        row_data.append(self._convert_metric(v, metric_types[i]))
                all_data.append(row_data)

            return all_data, names, dimension_types + metric_types

        def _request_report_api(self, limit: int, token: str, request: dict):

            request["pageSize"] = limit
            if token:
                request["pageToken"] = token

            try:
                response = self.parent.data_client.reports().batchGet(
                    body={
                        'reportRequests': [request],
                        'useResourceQuotas': False  # only for 360
                    }
                ).execute()
            except err.HttpError as e:
                data = json.loads(e.content.decode('utf-8'))
                code = data['error']["code"]
                message = data['error']['message']
                if code == 400:  # and "rate limit exceeded" in message.lower():
                    LOGGER.error(message)
                    raise errors.BadRequest(message)

            report_data = response.get('reports', [])[0]

            total_rows = report_data['data']['rowCount']

            samples_count = report_data['data'].get('samplesReadCounts')
            samples_size = report_data['data'].get('samplingSpaceSizes')
            next_token = report_data.get('nextPageToken', None)
            if samples_count:
                LOGGER.warn(f"samplesReadCounts = {samples_count}")
            if samples_size:
                LOGGER.warn(f"samplingSpaceSizes = {samples_size}")

            # try:
            data, headers, types = self._parse_response(report_data)
            # except:
            #     print(report_data)
            #     raise

            return data, total_rows, headers, types, next_token

        def _report_generator(self, request: dict, limit: int = 10000):
            """Send request to get report data"""
            if not self.parent.view.id:
                # LOGGER.error("Viewを先に選択してから実行してください。")
                return

            token = "0"
            while True:
                try:
                    (data, total_rows, headers, types, next_token) = self._request_report_api(limit, token, request)
                except err.HttpError as e:
                    value = str(sys.exc_info()[1])
                    if 'disabled' in value:
                        LOGGER.error("\nGCPのプロジェクトでAnalytics Reporting APIを有効化してください。")
                        return
                    elif 'Invalid value' in value:
                        LOGGER.error("レポート抽出条件の書式や内容を見直してください。")
                        return
                    else:
                        raise e

                if token == "0" and total_rows:
                    LOGGER.info(f"Total {total_rows} rows found.")

                    if total_rows == 10001:
                        if [d for d in request['dimensions'] if d['name'] == 'ga:clientId']:
                            # print("!! The returned data might be restricted.")
                            LOGGER.info("clientId is not officially supported by Google. Using this dimension in an "
                                        "Analytics report may thus result in unexpected & unexplainable behavior ("
                                        "such as restricting the report to exactly 10,000 or 10,001 rows).")
                    self.headers = headers
                    self.types = types

                if len(data):
                    LOGGER.info(f"(received row #{token}-{int(token) + len(data) - 1})")

                for row in data:
                    yield row

                if not next_token:
                    # no more data
                    break
                if len(data) == 0:
                    # API bug
                    raise errors.PartialDataReturned()

                token = next_token

        def show(self, dimensions: list, metrics: list, return_generator: Optional[bool] = None, **kwargs):
            """Get Analytics report data"""
            if not self.parent.view.id:
                LOGGER.error("Viewを先に選択してから実行してください。")
                return
            start_date = kwargs.get('start_date', self.start_date)
            end_date = kwargs.get('end_date', self.end_date)
            LOGGER.info(f"Requesting a report ({start_date} - {end_date})")

            request = self._format_request(
                dimensions=dimensions,
                metrics=metrics,
                start_date=start_date,
                end_date=end_date,
                dimension_filter=kwargs.get('dimension_filter'),
                metric_filter=kwargs.get('metric_filter'),
                order_bys=kwargs.get('order'),
                segments=kwargs.get('segments'),
                show_total=False,
                limit=kwargs.get('limit'),
            )
            # print(request)

            iterator = self._report_generator(request, kwargs.get('limit', 10000))

            if return_generator:
                return iterator

            try:
                return pd.DataFrame(list(iterator), columns=self.headers)
            except errors.PartialDataReturned:
                LOGGER.warn("APIのバグにより全データを取得できませんでした。")
                if start_date != end_date:
                    LOGGER.warn("1日毎にデータを分割して取得します。")
                    all_data = []
                    for date in utils.get_date_range(start_date, end_date):
                        print(f"...{date}", end='')
                        request['dateRanges'][0]['startDate'] = date
                        request['dateRanges'][0]['endDate'] = date
                        request['pageToken'] = "0"
                        iterator = self._report_generator(request, kwargs.get('limit', 10000))
                        data = list(iterator)
                        LOGGER.debug(f"{len(data)} rows")
                        all_data.extend(data)
                    return pd.DataFrame(all_data, columns=self.headers)


def cid_date_page(ga3, include_domains=None, include_pages=None, exclude_pages=None, page_regex=None):
    """
    元データ抽出：コンテンツ閲覧者
    対象・除外・変換条件を指定してpage_viewイベントを抽出しdataframeに変換
    """
    filter = []
    if include_domains:
        filter.append(f'hostname=~{include_domains}')
    if include_pages:
        filter.append(f'pagePath=~{include_pages}')
    if exclude_pages:
        filter.append(f'pagePath!~{exclude_pages}')
    dimension_filter = ";".join(filter)

    try:
        df = ga3.report.show(
            dimensions=[
                'clientId',
                'date',
                'sessionCount',
                'pagePath',
            ],
            dimension_filter=dimension_filter,
            metrics=[
                'entrances',
                'uniquePageviews',
                'exits',
            ],
            metric_filter='uniquePageviews>0',
            order='clientId,pagePath,date,sessionCount',
            # return_generator=True
            limit=10000)
    except errors.BadRequest as e:
        print(f"条件の書き方に問題があります：{e}")
        return
    else:
        # 値を変換
        if len(df):
            if page_regex:
                utils.format_df(df, [('pagePath', page_regex, '')])
            return df.rename(columns={
                'pagePath': 'page',
                'uniquePageviews': 'sessions',
            })


def to_page_cid(_df):
    """Pageと人でまとめて回遊を算出
    """
    df = _df.groupby(['page', 'clientId']).agg({
        'date': 'min',  # 初めて閲覧した日（再訪問とCVの判定で使う）
        'sessionCount': 'min',  # 初めて閲覧したセッション番号（CVの判定で使う）
        'entrances': 'max',  # 入口になったことがあれば1
        'sessions': 'sum',  # 累計セッション回数
        'exits': 'sum',  # 累計exits
    }).rename(columns={
        'date': 'first_visit_date',
        'sessionCount': 'first_session_count',
    }).reset_index()

    df['kaiyu'] = df.apply(lambda x: 0 if x['sessions'] == 1 & x['exits'] == 1 else 1, axis=1)

    return df


def cid_last_returned_date(ga3):
    """元データ抽出：再訪問した人の最終訪問日
    """
    df = ga3.report.show(
        dimensions=[
            'clientId',
            'date',
        ],
        dimension_filter='ga:sessionCount!=1',
        metrics=[
            'entrances',
        ],
        order='ga:clientId,ga:date',
    )

    # 人単位でまとめて最後に訪問した日を算出
    _df = df.groupby(['clientId']).max().rename(columns={'date': 'last_visit_date'})

    return _df.drop(['entrances'], inplace=False, axis=1)


def to_page_cid_return(df1, df2):
    """閲覧後の再訪問を判定
    """
    _df = pd.merge(
        df1.drop(['sessions', 'exits'], inplace=False, axis=1),
        df2,
        how='left',
        on='clientId')

    _df['returns'] = _df.apply(
        lambda x: 1 if (x['last_visit_date'] != '') & (x['last_visit_date'] != x['first_visit_date']) else 0, axis=1)

    return _df


def cv_cid(ga3, include_pages=None, metric_filter='ga:entrances<1'):
    """元データ抽出：入口以外で特定CVページに到達
    """
    filter = []
    if include_pages:
        filter.append(f'pagePath=~{include_pages}')
    dimension_filter = ";".join(filter)

    _df = ga3.report.show(
        dimensions=[
            'pagePath',
            'clientId',
            'date',
            'sessionCount',
        ],
        dimension_filter=dimension_filter,
        metrics=[
            'users',
        ],
        metric_filter=metric_filter,
        order='ga:pagePath,ga:clientId,ga:date',
    ).drop(['users'], axis=1)
    return _df


def to_cid_last_cv(df):
    """人単位でまとめて最後にCVした日を算出
    """
    _df = df[['clientId', 'date', 'sessionCount']].groupby(['clientId']).max()
    _df.rename(columns={
        'date': 'last_cv_date',
        'sessionCount': 'last_cv_session_count',
    }, inplace=True)
    return _df


def to_cv(df1, df2):
    """コンテンツ閲覧後のCVを判定
    """
    _df = pd.merge(
        df1.drop(['last_visit_date'], inplace=False, axis=1),
        df2,
        how='left',
        on='clientId')

    def calc_new_col(row):
        if row['last_cv_date']:
            if int(row['last_cv_date']) > int(row['first_visit_date']):
                return 1
            elif row['last_cv_date'] == row['first_visit_date'] and row['first_session_count'] < row['last_cv_session_count']:
                return 1
        return 0

    _df['cv'] = _df.fillna(0).apply(calc_new_col, axis=1)

    return _df


def to_page_participation(df):
    """Page単位でまとめる
    """
    _df = df.groupby('page').agg({
        'clientId': 'nunique',
        'entrances': 'sum',
        'kaiyu': 'sum',
        'returns': 'sum',
        'cv': 'sum',
    }).reset_index().sort_values('clientId', ascending=False)

    _df.rename(columns={
        'clientId': 'users',
        'entrances': 'entry_users',
        'kaiyu': 'kaiyu_users',
        'returns': 'return_users',
        'cv': 'cv_users',
        }, inplace=True)
    return _df.sort_values('users', ascending=False)


def get_page_title(ga3, include_domains=None, include_pages=None, exclude_pages=None, page_regex=None, title_regex=None):
    """ 対象・除外・変換条件を指定してpageとtitleを抽出 """
    filter = []
    if include_domains:
        filter.append(f'hostname=~{include_domains}')
    if include_pages:
        filter.append(f'pagePath=~{include_pages}')
    if exclude_pages:
        filter.append(f'pagePath!~{exclude_pages}')
    dimension_filter = ";".join(filter)

    try:
        df = ga3.report.show(
            dimensions=[
                'pagePath',
                'pageTitle',
            ],
            dimension_filter=dimension_filter,
            metrics=[
                'uniquePageviews',
            ],
            metric_filter='uniquePageviews>0',
            order='-uniquePageviews',
            limit=10000)
    except errors.BadRequest as e:
        print(f"条件の書き方に問題があります：{e}")
        return

    df.rename(columns={
        'pagePath': 'page',
        'pageTitle': 'title',
    }, inplace=True)

    # 値を変換
    if page_regex:
        utils.format_df(df, [('page', page_regex, '')])
    if title_regex:
        utils.format_df(df, [('title', title_regex, '')])

    # group byでまとめる
    return df.sort_values(['page', 'uniquePageviews'], ascending=False).groupby('page').first().reset_index()[
        ['page', 'title']]

