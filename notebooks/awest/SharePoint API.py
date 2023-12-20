# Databricks notebook source
import requests
from requests.auth import HTTPBasicAuth

site_url = 'https://defra.sharepoint.com/teams/Team1645'
list_name = '' #'Restricted_ELM_RPA_data_sharing/modelling_data_update/2022-update/'
file_name = "crome_2021.ctl"

username = "andrew.west@defra.gov.uk"

api_url = f"{site_url}/_api/web/lists/getbytitle('{list_name}')/items?$filter=FileLeafRef eq '{file_name}'"
api_url = f"{site_url}/_api/web/lists/getbytitle('{list_name}')/items"

response = requests.get(api_url, auth=HTTPBasicAuth(username, password), verify=False)

displayHTML(response.text)
