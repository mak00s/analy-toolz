"""
Functions for Google API
"""

from typing import List, Optional
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

    def __init__(self, api="oauth2", version="v2", scopes=['https://www.googleapis.com/auth/analytics.readonly'], *args,
                 **kwargs):
        """constructor"""
        self.api = api
        self.api_version = version
        self.scopes = scopes
        self.credentials = kwargs.get('credentials')
        self._service = None
        self.discovery_url = kwargs.get('discovery_url', DISCOVERY_URI)
        self.retries = kwargs.get('retries', 3)
        self.credential_cache_file = kwargs.get('credential_cache_file', "creden-cache.json")
        self.cache_dir = kwargs.get('cache_dir', ".")
        self.log = logging.getLogger("__name__")

    @property
    def service(self):
        """get or create a api service"""
        if self._service is None:
            # self.log.debug(f"Creating a service for {self.api} API")
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
            return service_method.execute(num_retries=retry_count)
        except errors.HttpError as e:
            code = e.resp.get('code')
            reason = ''
            message = ''
            try:
                data = json.loads(e.content.decode('utf-8'))
                code = data['error']["code"]
                message = data['error']['message']
                reason = data['error']['errors'][0]['reason']
            except:  # noqa
                pass

            if code == 403 and "rate limit exceeded" in message.lower():
                self.log.info("rate limit reached, sleeping for %s seconds", 2 ** retry_count)
                time.sleep(2 ** retry_count)
                return self.retry(service_method, retry_count + 1)
            elif code == 403 and ("accessNotConfigured" in reason or 'disabled' in message):
                self.log.error(message)
                raise
            else:
                self.log.warn(f"got HttpError (content={data}")
                raise
        except BrokenPipeError:
            self.log.info("BrokenPipeError occurred but attempting to retry")
            return self.retry(service_method, retry_count + 1)
        except KeyboardInterrupt:
            raise
        except:  # noqa
            self.log.exception("Failed to execute api method")
            raise

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


def _is_service_account_json(file: str):
    """Return true if the provided JSON file is for a service account."""
    with open(file, 'r') as f:
        return _is_service_account_key(f.read())


def _is_service_account_key(key_json_text: str):
    """Return true if the provided text is a JSON service credentials file."""
    try:
        key_obj = json.loads(key_json_text)
    except json.JSONDecodeError:
        return False
    if not key_obj or key_obj.get('type', '') != 'service_account':
        return False
    return True


def _run_auth_flow(client_secret_file: Optional[str], scopes: List[str], config: Optional[dict] = {}):
    """Run OAuth2 Flow"""
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
    code = input("以下の入力欄に貼り付けてエンターを押してください")
    flow.fetch_token(code=code)
    return flow.credentials


def get_credentials(json_file: Optional[str], scopes: List[str], cache_file: str = '', reset_cache=False):
    """Get Credentials"""

    # Service Account
    if _is_service_account_json(json_file):
        return service_account.Credentials.from_service_account_file(json_file, scopes=scopes)

    # OAuth
    cache_file = cache_file if cache_file else get_cache_filename_from_json(json_file)
    if not reset_cache:
        credentials = load_credentials_from_cache(cache_file, scopes)
        if credentials:
            return credentials
    # no cache found, so run OAuth flow
    credentials = _run_auth_flow(json_file, scopes)
    # save cache
    return save_credentials_to_cache(cache_file, credentials)


def get_cache_filename_from_json(source_file: str):
    """Name cache file based on the provided source file"""
    base_name = os.path.splitext(os.path.basename(source_file))[0]
    return f".{base_name}_cached-cred.json"


def save_credentials_to_cache(cache_file: str, credentials: Credentials):
    """Save Credentials to cache file
    """
    with open(cache_file, 'w') as w:
        print(f"saving credentials to {cache_file}")
        w.write(credentials.to_json())
    return credentials


def load_credentials_from_cache(cache_file: str, scopes: list):
    """Load Credentials from cache file
    """
    if os.path.isfile(cache_file):
        print(f"loading credentials from {cache_file}")
        return Credentials.from_authorized_user_file(cache_file, scopes=scopes)


def delete_credentials_cache(cache_file: str = "creden-cache.json"):
    """Delete Credentials cache file
    """
    if os.path.isfile(cache_file):
        print(f"deleting cache file {cache_file}")
        os.remove(cache_file)
