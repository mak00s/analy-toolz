"""
Functions for Google API
"""

import json
import logging
import os
import time

from googleapiclient import errors
from googleapiclient.discovery import build, DISCOVERY_URI
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


class GoogleApi(object):
    """Google API helper object"""

    def __init__(self, api="oauth2", version="v2", scopes=['https://www.googleapis.com/auth/analytics.readonly'], *args, **kwargs):
        """constructor"""
        self.api = api
        self.api_version = version
        self.scopes = scopes
        self.credentials = kwargs.get('credentials')
        # self.sub = kwargs.get('sub')
        self._service = None
        self.discovery_url = kwargs.get('discovery_url', DISCOVERY_URI)
        self.retries = kwargs.get('retries', 3)
        self.credential_cache_file = kwargs.get('credential_cache_file', "creden.json")
        self.cache_dir = kwargs.get('cache_dir', ".cache")
        self.log = logging.getLogger("GoogleApi")

    @property
    def service(self):
        """get or create a api service"""
        if self._service is None:
            print("Creating a service.")
            self._service = build(self.api,
                                  self.api_version,
                                  credentials=self.credentials,
                                  # cache=program_memory_cache,
                                  discoveryServiceUrl=self.discovery_url)
        return self._service

    def auth(self, file: str):
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)
        cache_path = os.path.join(self.cache_dir, self.credential_cache_file)

        credentials = get_credentials(file, self.scopes, cache_path)

        self.credentials = credentials
        self._service = None
        return self

    def retry(self, service_method, retry_count=0):
        """
        retry a google api call and check for rate limits
        """
        try:
            ret = service_method.execute(num_retries=retry_count)
        except errors.HttpError as error:
            code = error.resp.get('code')

            reason = ''
            message = ''
            try:
                data = json.loads(error.content.decode('utf-8'))
                code = data['error']["code"]
                message = data['error']['message']
                reason = data['error']['errors'][0]['reason']
            except:  # noqa
                pass

            if code == 403 and "rate limit exceeded" in message.lower():
                self.log.info("rate limit reached, sleeping for %s seconds", 2 ** retry_count)
                time.sleep(2 ** retry_count)
                ret = self.retry(service_method, retry_count + 1)
            else:
                self.log.warn("got http error {} ({}): {}".format(code, reason, message))
                raise
        except KeyboardInterrupt:
            raise
        except:  # noqa
            self.log.exception("Failed to execute api method")
            raise
        return ret

    def __getattr__(self, name):
        """ get attribute or service wrapper
        :param name: attribute / service name
        :return:
        """
        return getattr(MethodHelper(self, self.service), name)

    @classmethod
    def ga_reporting(cls, version="v4"):
        """Google Analytics Reporting API v4"""
        return GoogleApi("analyticsreporting", version, ["https://www.googleapis.com/auth/analytics.readonly"])

    @classmethod
    def ga_management(cls, version="v3"):
        """Google Analytics Management API v3"""
        return GoogleApi("analytics", version, ["https://www.googleapis.com/auth/analytics.readonly"])


class MethodHelper(object):
    """ helper to streamline api calls"""

    def __init__(self, google_api, service, name=None, path=None):
        """
        create a method helper
        :param google_api GoogleApi instance of api
        :param service Google API service (GoogleApi.service) or method of it
        :param name method name
        :param path API path i.e. for compute: instances.list
        """
        self.google_api = google_api
        self.service = service
        self.name = name
        self.path = path if path is not None else []
        if name is not None:
            self.path.append(name)
        # print("constructor %s", name)

    def execute(self, *args, **kwargs):
        """execute service api"""
        # self.log.info("execute %s", self.name)
        return self.google_api.retry(self.service)

    def call(self, *args, **kwargs):
        """
        wrapper for service methods
        this wraps an GoogleApi.service call so the next level can also use helpers
        i.e. for compute v1 api GoogleApi.service.instances() can be used as Google.instances()
        and will return a MethodHelper instance
        """
        # self.log.info("call %s", self.name)
        return MethodHelper(self.google_api, getattr(self.service, self.name)(*args, **kwargs))

    def __getattr__(self, name):
        """ get service method """
        # self.log.info("getattr %s", name)
        if not hasattr(self.service, name):
            err_msg = u"API method {} unknown on {} {}".format(u".".join(self.path + [name]),
                                                               self.google_api.api,
                                                               self.google_api.api_version)
            raise RuntimeError(err_msg)
        return MethodHelper(self.google_api, self.service, name, self.path).call


def _is_service_account_json(file):
    """Return true if the provided JSON file is for a service account."""
    with open(file, 'r') as f:
        return _is_service_account_key(f.read())


def _is_service_account_key(key_json_text):
  """Return true if the provided text is a JSON service credentials file."""
  try:
    key_obj = json.loads(key_json_text)
  except json.JSONDecodeError:
    return False
  if not key_obj or key_obj.get('type', '') != 'service_account':
    return False
  return True


def _run_auth_flow(client_secret_file, scopes, config=None):
    """ Run OAuth2 Flow
    """
    if os.path.exists(client_secret_file):
        flow = InstalledAppFlow.from_client_secrets_file(
            client_secret_file,
            scopes=scopes,
            redirect_uri="urn:ietf:wg:oauth:2.0:oob"
        )
    else:
        print("JSON not found.")
        flow = InstalledAppFlow.from_client_config(
            config,
            scopes=scopes,
            redirect_uri="urn:ietf:wg:oauth:2.0:oob"
        )
    auth_url, _ = flow.authorization_url(prompt="consent")
    print("以下のURLをクリックし、Google認証後に表示される文字列をコピーし、")
    print(auth_url)
    time.sleep(4)
    code = input("以下の入力欄に貼り付けてエンターキーを押してください")
    flow.fetch_token(code=code)
    credentials = flow.credentials
    return credentials


def get_credentials(file: str, scopes, cache_path: str):
    """Get Credentials
    """
    if _is_service_account_json(file):
        credentials = service_account.Credentials.from_service_account_file(file)
    else:
        if os.path.isfile(cache_path):
            # use cache
            print(f"loading cache from {cache_path}")
            credentials = Credentials.from_authorized_user_file(cache_path, scopes=scopes)
        else:
            credentials = _run_auth_flow(file, scopes)
            # save cache
            print(f"saving cache to {cache_path}")
            with open(cache_path, 'w') as file:
                file.write(credentials.to_json())

    return credentials
