# Databricks notebook source
# MAGIC %sh
# MAGIC pip install -qU pip python-dotenv Office365-REST-Python-Client

# COMMAND ----------

from getpass import getpass
import dotenv
from cryptography.fernet import Fernet

from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.sharepoint.folders.folder import Folder

import requests
import contextlib
import warnings
from urllib3.exceptions import InsecureRequestWarning


class SPOL:
    '''SharePoint OnLine sync tool.
    Connects to SPOL using dotenv or manually enter your password.

    '''
    def __init__(self, **kwargs):
        self.spol_base_url = 'https://defra.sharepoint.com/'
        self.spol_username = self.get_user()
        self.spol_password = self.get_pass()
        self.default_location = '/dbfs/FileStore/'
        self.__dict__.update(kwargs)
        self.ctx = self.get_context()
        self.ssl = self.check_ssl()

    # .env
    def encrypt_value(value):
        return Fernet(dotenv.get_key('.env', 'SHA_KEY')).encrypt(value.encode('utf8'))

    def decrpyt_key(key):
        return Fernet(dotenv.get_key('.env', 'SHA_KEY')).decrypt(dotenv.get_key('.env', key)).decode('utf8')

    def get_user():
        return dotenv.get_key('.env', 'SPOL_USER')

    def get_pass():
        return self.decrpyt_key('SPOL_PASS_SHA')

    # setup
    def get_context(self):
        return ClientContext(spol_base_url).with_user_credentials(spol_username, spol_password)

    def check_ssl(self):
        try:
            self.ctx.web.get().execute_query().url
            return lambda: None
        except Exception:
            with no_ssl_verification():
                self.ctx.web.get().execute_query().url
            return no_ssl_verification

    # tools
    @self.ssl
    def list_files(self, target):
        files = self.ctx.web.get_folder_by_server_relative_path(target).get_files(True).execute_query()
        return [f.properties['ServerRelativeUrl'] for f in files]

    @self.ssl
    def dl_file(self, target):
        response = File.open_binary(self.ctx, target)
        filename = self.default_location + target.split('/')[-1]
        with open(filename, 'wb') as f:
            f.write(response.content)

    @self.ssl
    def dl_folder(ctx, target):
        pass


@contextlib.contextmanager
def no_ssl_verification():
    opened_adapters = set()
    original_merge_environment_settings = requests.Session.merge_environment_settings

    def merge_environment_settings(self, url, proxies, stream, verify, cert):
        opened_adapters.add(self.get_adapter(url))

        settings = original_merge_environment_settings(self, url, proxies, stream, verify, cert)
        settings['verify'] = False

        return settings

    requests.Session.merge_environment_settings = merge_environment_settings

    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', InsecureRequestWarning)
            yield
    finally:
        requests.Session.merge_environment_settings = original_merge_environment_settings
        for adapter in opened_adapters:
            try:
                adapter.close()
            except:
                pass


# COMMAND ----------

import geopandas as gpd

with no_ssl_verification():
    ctx = spol_ctx()
    target = '/teams/Team1645/Restricted_ELM_RPA_data_sharing/modelling_data_update/2023-update/Live_Parcel_Links.zip'
    pl_file(ctx, target)
df

# COMMAND ----------

# MAGIC %sh
# MAGIC # unzip Live_Parcel_Links
# MAGIC ls -lh
