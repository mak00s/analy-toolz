"""Megaton GA"""

from collections import defaultdict
from ipywidgets import interact
from IPython.display import clear_output
import pandas as pd
import sys

from google.api_core.exceptions import ServiceUnavailable

try:
    import itables

    itables.init_notebook_mode()
except:
    if 'google.colab' in sys.modules:
        from . import colabo

from . import constants, errors, ga3, ga4, google_api, gsheet, utils, widget


class Launch(object):
    def __init__(self, json):
        """constructor"""
        self.json = json
        self.creds = None
        self.ga_ver = None
        self.ga3 = None
        self.ga4 = None
        self.gs = None
        self.is_colab = False
        self.content_analysis = None
        if 'google.colab' in sys.modules:
            self.is_colab = True
            colabo.init()
        if json:
            self.auth()

    def auth(self):
        """GCS認証"""
        self.creds = google_api.get_credentials(self.json, constants.DEFAULT_SCOPES)
        try:
            self.ga4 = ga4.LaunchGA4(self.creds)
        except ServiceUnavailable:
            if 'invalid_grant' in str(sys.exc_info()[1]):
                print(f"期限が切れたようなので、もう一度認証します。")
                self.creds = google_api.get_credentials(self.json, constants.DEFAULT_SCOPES, reset_cache=True)

        except Exception as e:
            raise e

    def show(self, df):
        """Display pandas DaraFrame as a table"""
        if self.is_colab:
            return colabo.table(df)
        try:
            itables.show(df)
        except:
            display(df)

    @staticmethod
    def clear():
        """Clear output of a Jupyter Notebook cell"""
        clear_output()

    """Google Analytics
    """

    def launch_ga4(self):
        """GA4の準備"""
        self.ga4 = ga4.LaunchGA4(self.creds)
        self.select_ga4_property()

    def select_ga4_property(self):
        """GA4のアカウントとプロパティをメニューで選択"""
        clear_output()
        if self.ga4.accounts:
            print("　　↓GA4のアカウントとプロパティを以下から選択してください")
            menu1, menu2, _ = widget.create_ga_account_property_menu(self.ga4.accounts)

            @interact(value=menu1)
            def menu1_selected(value):
                if value:
                    self.ga4.account.select(value)
                    prop = [d for d in self.ga4.accounts if d['id'] == value][0]['properties']
                    menu2.options = [(n['name'], n['id']) for n in prop]
                else:
                    self.ga4.account.select(None)
                    menu2.options = [('---', '')]

            @interact(value=menu2)
            def menu2_selected(value):
                if value:
                    self.ga4.property.select(value)
                    print(f"　Property ID：{self.ga4.property.id}、",
                          f"作成日：{self.ga4.property.created_time.strftime('%Y-%m-%d')}")
                    self.ga_ver = 4
        else:
            print("権限が付与されたGA4アカウントが見つかりません。")

    def select_ga4_dimensions_and_metrics(self):
        """JupyterLab（Google Colaboratory）用にメニューを表示"""
        import panel as pn

        groups_dim = defaultdict(lambda: {})
        groups_dim[' ディメンションを選択してください']['---'] = ''
        for i in self.ga4.property.get_dimensions():
            groups_dim[i['category']][i['display_name']] = i['api_name']

        dim = pn.widgets.Select(
            # name='Dimensions',
            groups=groups_dim,
            value='eventName'
        )

        groups_met = defaultdict(lambda: {})
        groups_met[' 指標を選択してください']['---'] = ''
        for i in self.ga4.property.get_metrics():
            groups_met[i['category']][i['display_name']] = i['api_name']

        met = pn.widgets.Select(
            # name='Metrics',
            groups=groups_met,
            value='eventCount'
        )

        return dim, met

    def launch_ga(self):
        """GA (UA)の準備"""
        self.ga3 = ga3.LaunchUA(self.creds, credential_cache_file=google_api.get_cache_filename_from_json(self.json))
        self.select_ga3_view()

    def select_ga3_view(self):
        """GAのアカウントとプロパティとビューをメニューで選択"""
        clear_output()
        if self.ga3.accounts:
            print("　　↓GAのアカウントとプロパティを以下から選択してください")
            menu1, menu2, menu3 = widget.create_ga_account_property_menu(self.ga3.accounts)

            @interact(value=menu1)
            def menu1_selected(value):
                if value:
                    self.ga3.account.select(value)
                    opt = [d for d in self.ga3.accounts if d['id'] == value][0]['properties']
                    menu2.options = [(n['name'], n['id']) for n in opt]
                else:
                    self.ga3.account.select(None)
                    menu2.options = [('---', '')]
                    menu3.options = [('---', '')]

            @interact(value=menu2)
            def menu2_selected(value):
                if value:
                    self.ga3.property.select(value)
                    menu3.options = [(d['name'], d['id']) for d in self.ga3.property.views if d['property_id'] == value]
                else:
                    self.ga3.property.select(None)
                    menu3.options = [('---', '')]

            @interact(value=menu3)
            def menu3_selected(value):
                if value:
                    self.ga3.view.select(value)
                    print(f"View ID：{self.ga3.view.id}、作成日：{self.ga3.view.created_time}")
                    self.ga_ver = 3
        else:
            print("権限が付与されたGAアカウントが見つかりません。")

    def set_dates(self, date1, date2):
        """レポート期間をセット"""
        if self.ga_ver == 3:
            self.ga3.report.set_dates(date1, date2)
        elif self.ga_ver == 4:
            self.ga4.report.set_dates(date1, date2)
        print(f"GA{self.ga_ver}のレポート期間：{date1}〜{date2}")

    @property
    def dates_as_string(self):
        if self.ga_ver == 3:
            start_date = self.ga3.report.start_date
            end_date = self.ga3.report.end_date
        elif self.ga_ver == 4:
            start_date = self.ga4.report.start_date
            end_date = self.ga4.report.end_date
        return f"{start_date.replace('-', '')}-{end_date.replace('-', '')}"

    def report(self, d: list, m: list, filter_d=None, filter_m=None, sort=None, **kwargs):
        """GA/GA4からデータを抽出"""
        dimensions = [i for i in d if i]
        metrics = [i for i in m if i]
        if self.ga_ver == 3:
            return self.ga3.report.show(
                dimensions,
                metrics,
                dimension_filter=filter_d,
                metric_filter=filter_m,
                order_bys=sort,
                segments=kwargs.get('segments'),
            )
        elif self.ga_ver == 4:
            return self.ga4.report.run(
                dimensions,
                metrics,
                dimension_filter=filter_d,
                metric_filter=filter_m,
                order_bys=sort,
            )

    """Download
    """

    def save(self, df: pd.DataFrame, filename: str):
        """データを保存：ファイル名に期間を付与。拡張子がなければ付与"""
        new_filename = utils.append_suffix_to_filename(filename, f"_{self.dates_as_string}")
        utils.save_df(df, new_filename)
        # print(f"CSVファイル{new_filename}を保存しました。")
        return new_filename

    def download(self, df: pd.DataFrame, filename: str):
        """データを保存し、Colabからダウンロード"""
        new_filename = self.save(df, filename)
        colabo.download(new_filename)

    """Google Sheets
    """

    def launch_gs(self, url):
        """APIでGoogle Sheetsにアクセスする準備"""
        try:
            self.gs = gsheet.LaunchGS(self.creds, url)
        except errors.BadCredentialFormat:
            print("認証情報のフォーマットが正しくないため、Google Sheets APIを利用できません。")
        except errors.BadCredentialScope:
            print("認証情報のスコープ不足のため、Google Sheets APIを利用できません。")
        except errors.BadUrlFormat:
            print("URLのフォーマットが正しくありません")
        except errors.ApiDisabled:
            print("Google SheetsのAPIが有効化されていません。")
        except errors.UrlNotFound:
            print("URLが見つかりません。")
        except errors.BadPermission:
            print("該当スプレッドシートを読み込む権限がありません。")
        except Exception as e:
            raise e
        else:
            if self.gs.title:
                print(f"Googleスプレッドシート「{self.gs.title}」を開きました。")
                return True

    def select_sheet(self, sheet_name):
        try:
            name = self.gs.sheet.select(sheet_name)
            if name:
                print(f"「{sheet_name}」シートを選択しました。")
                return True
        except errors.SheetNotFound:
            print(f"{sheet_name} シートが存在しません。")

    def load_cell(self, row, col, what: str = None):
        self.gs.sheet.cell.select(row, col)
        value = self.gs.sheet.cell.data
        if what:
            print(f"{what}は{value}")
        return value

    """Analysis
    """

    def analyze_content(self, url):
        """コンテンツ貢献度分析"""
        self.content_analysis = self.ContentAnalysis(self, url)

    class ContentAnalysis:
        """コンテンツ貢献度分析"""

        def __init__(self, parent, url: str, sheet_name: str = '使い方'):
            """constructor"""
            self.parent = parent
            self.url = url
            self.sheet_name = sheet_name
            self.data = {}
            self.conf = {}
            if self.update():
                self.show()

        def _get_config(self):
            """設定をシートから読み込む"""
            # open Google Sheets
            self.parent.launch_gs(self.url)
            if self.parent.select_sheet(self.sheet_name):
                # 設定をセルから読み込む
                template_ver = self.parent.load_cell(1, 4)
                if template_ver == '2':
                    self.conf['include_domains'] = self.parent.load_cell(5, 6)
                    self.conf['include_pages'] = self.parent.load_cell(9, 6)
                    self.conf['exclude_pages'] = self.parent.load_cell(13, 6)
                    self.conf['min_pv'] = utils.extract_integer_from_string(self.parent.load_cell(16, 6))
                    self.conf['cv_pages'] = self.parent.load_cell(20, 7)
                    self.conf['m1_pages'] = self.parent.load_cell(23, 7)
                    self.conf['m2_pages'] = self.parent.load_cell(24, 7)
                    self.conf['m3_pages'] = self.parent.load_cell(25, 7)
                    self.conf['m4_pages'] = self.parent.load_cell(26, 7)
                    self.conf['m5_pages'] = self.parent.load_cell(27, 7)
                    self.conf['page_regex'] = self.parent.load_cell(30, 6)
                    self.conf['title_regex'] = self.parent.load_cell(33, 6)
                else:
                    self.conf['include_domains'] = self.parent.load_cell(5, 5)
                    self.conf['include_pages'] = self.parent.load_cell(11, 5)
                    self.conf['exclude_pages'] = self.parent.load_cell(16, 5)
                    self.conf['min_pv'] = utils.extract_integer_from_string(self.parent.load_cell(21, 5))
                    self.conf['cv_pages'] = self.parent.load_cell(26, 5)
                    self.conf['page_regex'] = self.parent.load_cell(29, 5)
                    self.conf['title_regex'] = self.parent.load_cell(32, 5)
                self.conf['metrics'] = [i for i in ['cv','m1','m2','m3','m4','m5'] if self.conf[f'{i}_pages']]

        def _get_page_cid(self):
            """対象コンテンツが閲覧されたcidを得る"""
            if self.parent.ga_ver == 3:
                # 元データ抽出：対象pageが閲覧されたcidとdate
                _df = ga3.get_cid_date_page(self.parent.ga3, self.conf)
                print(f"（{len(_df)}行の元データを抽出...）")

                # pageとcidでまとめる
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

                # 回遊を算出
                df['kaiyu'] = df.apply(lambda x: 0 if x['sessions'] == 1 & x['exits'] == 1 else 1, axis=1)
                return df

        def _add_return_to_page_cid(self):
            """対象page閲覧後の再訪問の指標を追加する"""
            if self.parent.ga_ver == 3:
                # 元データ抽出：再訪問した人の最終訪問日
                _df = ga3.get_last_returned_date(self.parent.ga3)

                # 閲覧後の再訪問を追加
                df = pd.merge(
                    self.data['page_cid'].drop(['sessions', 'exits'], inplace=False, axis=1),
                    _df,
                    how='left',
                    on='clientId')

                df['returns'] = df.apply(
                    lambda x: 1 if (x['last_visit_date'] != '') & (
                            x['last_visit_date'] != x['first_visit_date']) else 0, axis=1)

                print("（再訪問の指標を追加...）")
                return df.drop(['last_visit_date'], inplace=False, axis=1)

        def _add_cv_to_page_cid(self, cv_pages, cv_label: str = 'cv'):
            """対象page閲覧後に指定CVページに到達した人数を追加する"""
            if self.parent.ga_ver == 3:
                # 元データ抽出：入口以外でCVページに到達したcidとdate
                _df = ga3.get_no_entrance_cv_cid(self.parent.ga3, cv_pages)

                # cidでまとめて最後にCVしたdateを算出
                df = _df[['clientId', 'date', 'sessionCount']].groupby(['clientId']).max()
                df.rename(columns={
                    'date': f'last_{cv_label}_date',
                    'sessionCount': f'last_{cv_label}_session_count',
                }, inplace=True)

                # コンテンツ閲覧後のCVを判定
                df2 = pd.merge(
                    self.data['page_cid'],
                    df,
                    how='left',
                    on='clientId')

                def calc_new_col(row):
                    if row[f'last_{cv_label}_date']:
                        if int(row[f'last_{cv_label}_date']) > int(row['first_visit_date']):
                            return 1
                        elif row[f'last_{cv_label}_date'] == row['first_visit_date'] \
                                and row['first_session_count'] < row[f'last_{cv_label}_session_count']:
                            return 1
                    return 0

                df2[cv_label] = df2.fillna(0).apply(calc_new_col, axis=1)

                print(f"（{cv_label}の指標を追加...）")
                return df2.drop([f'last_{cv_label}_date', f'last_{cv_label}_session_count'], inplace=False, axis=1)

        def _group_by_page(self, df, cv_label: str = 'cv'):
            """Page単位でまとめる"""
            if self.parent.ga_ver == 3:
                d = {
                    'clientId': 'nunique',
                    'entrances': 'sum',
                    'kaiyu': 'sum',
                    'returns': 'sum',
                    # cv_label: 'sum',
                }
                for i in self.conf['metrics']:
                    d[i] = 'sum'
                _df = df.groupby('page').agg(d).reset_index().sort_values('clientId', ascending=False)

                c = {
                    'clientId': 'users',
                    'entrances': 'entry_users',
                    'kaiyu': 'kaiyu_users',
                    'returns': 'return_users',
                    # cv_label: f'{cv_label}_users',
                }
                for i in self.conf['metrics']:
                    c[i] = f'{i}_users'
                _df.rename(columns=c, inplace=True)

                return _df[_df['users'] >= self.conf['min_pv']].reset_index()

        def _add_title_to_page(self):
            """ページタイトルを取得・変換して追加する"""
            if self.parent.ga_ver == 3:
                # 元データ抽出：タイトル
                _df = ga3.get_page_title(self.parent.ga3, self.conf)

                df = pd.merge(
                    self.data['page'],
                    _df,
                    how='left',
                    on='page')

                return df

        def update(self):
            """コンテンツ貢献度のレポートを作成する"""

            # 設定をシートから読み込む
            self._get_config()

            # 一番細かい元データを得る
            try:
                self.data['page_cid'] = self._get_page_cid()
            except errors.BadRequest as e:
                print(f"対象ドメイン・ページの指定方法に問題があります：{self.conf}")
                return
            except errors.NoDataReturned:
                print(f"データが見つかりません。条件を見直してください：{self.conf}")
                return

            # 指標を算出する
            self.data['page_cid'] = self._add_return_to_page_cid()
            for i in self.conf['metrics']:
                self.data['page_cid'] = self._add_cv_to_page_cid(self.conf[f'{i}_pages'], i)

            # pageでまとめる
            self.data['page'] = self._group_by_page(
                self.data['page_cid'].drop(['first_visit_date', 'first_session_count'], inplace=False, axis=1)
            )

            # タイトルを追加する
            self.data['page'] = self._add_title_to_page()

            # 順番を並び替える
            columns = ['page', 'title', 'users', 'entry_users', 'kaiyu_users', 'return_users']
            for i in self.conf['metrics']:
                columns.append(f'{i}_users')
            self.data['page'] = self.data['page'][columns]

            return True

        def show(self):
            """コンテンツ貢献度のレポートを表示する"""
            self.parent.show(self.data['page'])

        def save(self, sheet_name: str = '_cont'):
            """コンテンツ貢献度のレポートをGoogle Sheetsへ反映する"""
            if self.parent.select_sheet(sheet_name):
                if self.parent.gs.sheet.overwrite_data(self.data['page'], include_index=False):
                    self.parent.gs.sheet.auto_resize(cols=[2, 3, 4, 5, 6, 7])
                    self.parent.gs.sheet.resize(col=1, width=300)
                    self.parent.gs.sheet.resize(col=2, width=300)
                    self.parent.gs.sheet.freeze(rows=1)
                    print(f"レポートのデータを上書き保存しました。")

    def save_content_analysis_to_gs(self, df, sheet_name: str = '_cont'):
        """OLD"""
        if self.select_sheet(sheet_name):
            if self.gs.sheet.overwrite_data(df, include_index=False):
                self.gs.sheet.auto_resize(cols=[2, 3, 4, 5, 6, 7])
                self.gs.sheet.resize(col=1, width=300)
                self.gs.sheet.resize(col=2, width=300)
                self.gs.sheet.freeze(rows=1)
                print(f"レポートのデータを上書き保存しました。")
