# Databricks notebook source
# MAGIC %sh
# MAGIC pip install -qU pip python-dotenv Office365-REST-Python-Client

# COMMAND ----------

import dotenv
from cryptography.fernet import Fernet

from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.sharepoint.folders.folder import Folder

import requests
import contextlib
import warnings
from urllib3.exceptions import InsecureRequestWarning


def hash_pass(value):
    return Fernet(dotenv.get_key('.env', 'SHA_KEY')).encrypt(value.encode('utf8'))

def get_hash_pass(key):
    return Fernet(dotenv.get_key('.env', 'SHA_KEY')).decrypt(dotenv.get_key('.env', key)).decode('utf8')

def spol_ctx(
    spol_base_url:str = 'https://defra.sharepoint.com/teams/Team1645',
    spol_username:str = dotenv.get_key('.env', 'SPOL_USER'),
    spol_password:str = get_hash_pass('SPOL_PASS_SHA'),
):
    ctx = ClientContext(spol_base_url).with_user_credentials(spol_username, spol_password)
    ctx.web.get().execute_query().url  # Test
    return ctx


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


with no_ssl_verification():
    ctx = spol_ctx()

# COMMAND ----------

def enum_folder(parent_folder, fn):
    """
    :type parent_folder: Folder
    :type fn: (File)-> None
    """
    parent_folder.expand(["Files", "Folders"]).get().execute_query()
    for file in parent_folder.files:  # type: File
        fn(file)
    for folder in parent_folder.folders:  # type: Folder
        enum_folder(folder, fn)


def print_file(f):
    """
    :type f: File
    """
    print(f.properties['ServerRelativeUrl'])


with no_ssl_verification():
    ctx = spol_ctx()
    target = "Services"
    files = ctx.web.get_folder_by_server_relative_path(target).get_files(True).execute_query()
    [print_file(f) for f in files]
