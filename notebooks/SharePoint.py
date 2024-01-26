# Databricks notebook source
# MAGIC %sh
# MAGIC pip install -qU pip python-dotenv Office365-REST-Python-Client

# COMMAND ----------

import os
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
        self.dl_location = '/dbfs/FileStore/SharePoint/'
        self.env = '/Workspace/Repos/andrew.west@defra.gov.uk/elmo_geo/.env'
        self.base_url = 'https://defra.sharepoint.com/'
        self.username = self.get_user()
        self.password = self.get_pass()
        self.__dict__.update(kwargs)
        self.ctx = self.get_context()
        self.ssl = self.check_ssl()
        os.makedirs(self.dl_location, exist_ok=True)

    # .env
    def encrypt_value(self, value):
        return Fernet(dotenv.get_key(self.env, 'SHA_KEY')).encrypt(value.encode('utf8'))

    def decrpyt_key(self, key):
        return Fernet(dotenv.get_key(self.env, 'SHA_KEY')).decrypt(dotenv.get_key(self.env, key)).decode('utf8')

    def get_user(self):
        return dotenv.get_key(self.env, 'SPOL_USER')

    def get_pass(self):
        return self.decrpyt_key('SPOL_PASS_SHA')

    # setup
    def get_context(self):
        return ClientContext(self.base_url).with_user_credentials(self.username, self.password)

    def check_ssl(self):
        try:
            self.ctx.web.get().execute_query().url
            return lambda: None
        except Exception:
            with no_ssl_verification():
                self.ctx.web.get().execute_query().url
            print('Using no_ssl_verification')
            return no_ssl_verification

    # tools
    def list_files(self, target):
        with self.ssl():
            files = self.ctx.web.get_folder_by_server_relative_path(target).get_files(True).execute_query()
            return [f.properties['ServerRelativeUrl'] for f in files]

    def dl_file(self, target):
        with self.ssl():
            filename = self.dl_location + target.split('/')[-1]
            self.ctx.web.get_file_by_server_relative_url(target).download(filename).execute_query()

    def dl_folder(self, target):
        with self.ssl():
            pass

    def up_file(self, filepath, target):
        with self.ssl():
            pass

    def up_folder(self, folderpath, target):
        with self.ssl():
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

spol = SPOL(base_url = 'https://defra.sharepoint.com/teams/Team1645/')
with no_ssl_verification():
    target = 'Restricted_ELM_RPA_data_sharing/modelling_data_update/2023-update/rpa-parcel-2023_12_13.parquet.zip'
    filepath = '/dbfs/FileStore/SharePoint/rpa-parcel-2023_12_13.parquet.zip'
    file = spol.ctx.web.get_file_by_server_relative_path(target).download(filepath).execute_query()
file
