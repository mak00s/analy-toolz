"""Megaton GA"""

from collections import defaultdict
from ipywidgets import interact
from IPython.display import clear_output
import pandas as pd
import sys

from google.api_core.exceptions import ServiceUnavailable

from . import constants, errors, ga3, ga4, google_api, gsheet, utils, widget


class Launch(object):
    def __init__(self, json):
        """constructor"""
        self.creds = None
        self.ga3 = None
        self.ga4 = None
        self.ga_ver = None
        self.gs = None
        self.json = json
        if 'google.colab' in sys.modules:
            self.is_colab = True
            from . import colabo
            colabo.init()
        else:
            self.is_colab = False
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
        else:

    """Google Analytics
    """
    def launch_ga4(self):
        """GA4の準備"""
        self.ga4 = ga4.LaunchGA4(self.creds)
        self.select_ga4_property()

    def select_ga4_property(self):
        """GA4のアカウントとプロパティを選択"""
        clear_output()
        if self.ga4.accounts:
            print("　　↓GAのアカウントとプロパティを以下から選択してください")
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
        self.ga3 = ga3.Megaton(self.creds, credential_cache_file=google_api.get_cache_filename_from_json(self.json))
        self.select_ga3_view()

    def select_ga3_view(self):
        """GAのアカウントとプロパティとビューを選択"""
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
        print(f"GA{self.ga_ver}のレポート期間は{date1}〜{date2}")

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

    @property
    def dates_as_string(self):
        if self.ga_ver == 3:
            start_date = self.ga3.report.start_date
            end_date = self.ga3.report.end_date
        elif self.ga_ver == 4:
            start_date = self.ga4.report.start_date
            end_date = self.ga4.report.end_date
        return f"{start_date.replace('-', '')}-{end_date.replace('-', '')}"

    def save(self, df: pd.DataFrame, filename: str, format: str = 'CSV'):
        """データを保存：ファイル名に期間を付与。拡張子がなければ付与"""
        new_filename = utils.append_suffix_to_filename(filename, f"_{self.dates_as_string}")
        utils.save_df(df, new_filename)
        print(f"CSVファイル{new_filename}を保存しました。")

    def analyze_content(self, sheet_name: str = '使い方'):
        """コンテンツ貢献度分析"""
        # 設定をシートから読み込む
        if self.select_sheet(sheet_name):
            # 設定を読み込む
            include_domains = self.load_cell(5, 5)
            include_pages = self.load_cell(11, 5)
            exclude_pages = self.load_cell(16, 5)
            cv_pages = self.load_cell(26, 5)
            page_regex = self.load_cell(29, 5)
            title_regex = self.load_cell(32, 5)

            # 元データ抽出：コンテンツ閲覧者
            _df = ga3.cid_date_page(self.ga3, include_domains, include_pages, exclude_pages, page_regex)
            if not len(_df):
                print("データがありません。")
                return

            # Pageと人でまとめて回遊を算出
            df = ga3.to_page_cid(_df)

            # 元データ抽出：再訪問した人の最終訪問日
            _df = ga3.cid_last_returned_date(self.ga3)

            # 閲覧後の再訪問を判定
            df2 = ga3.to_page_cid_return(df, _df)

            # 元データ抽出：入口以外で特定CVページに到達
            _df = ga3.cv_cid(self.ga3, cv_pages)

            # 人単位でまとめて最後にCVした日を算出
            _df = ga3.to_cid_last_cv(_df)

            # コンテンツ閲覧後のCVを判定
            df3 = ga3.to_cv(df2, _df)

            # Page単位でまとめる
            df_con = ga3.to_page_participation(df3[['page', 'clientId', 'entrances', 'kaiyu', 'returns', 'cv']])

            # 元データ抽出：タイトル
            df_t = ga3.get_page_title(self.ga3, include_domains, include_pages, exclude_pages, page_regex, title_regex)

            df = pd.merge(
                df_con,
                df_t,
                how='left',
                on='page')[['page', 'title', 'users', 'entry_users', 'kaiyu_users', 'return_users', 'cv_users']]

            return df

    def save_content_analysis_to_gs(self, df, sheet_name: str = '_cont'):
        if self.select_sheet(sheet_name):
            if self.gs.sheet.overwrite_data(df, include_index=False):
                self.gs.sheet.auto_resize(cols=[2, 3, 4, 5, 6, 7])
                self.gs.sheet.resize(col=1, width=300)
                self.gs.sheet.resize(col=2, width=300)
                self.gs.sheet.freeze(rows=1)
                print(f"レポートのデータを上書き保存しました。")

    def user_explorer(self):
        pass

    """Google Sheets
    """
    def launch_gs(self, url):
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
        except errors.SheetNotFound:
            print(f"{sheet_name} シートが存在しません。")
        if name:
            print(f"「{sheet_name}」シートを選択しました。")
            return True

    def load_cell(self, row, col, what: str = None):
        self.gs.sheet.cell.select(row, col)
        value = self.gs.sheet.cell.data
        if what:
            print(f"{what}は{value}")
        return value
