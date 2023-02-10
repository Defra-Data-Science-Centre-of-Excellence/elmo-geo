# elmo-geo

## Setup to develop using VS Code and dbx sync

On your SCE VM...

Clone the repo

```{bash}
git clone git@github.com:Defra-Data-Science-Centre-of-Excellence/elmo-geo.git
```

Install azure cli

```{bash}
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

Login to Azure CLI

```{bash}
az login --tenant bce3d7d1-cbbd-481e-8c81-eaecfc38b551 --use-device-code
```

Login to databricks by authenticating with ADD

Install databricks and dbx in your virtual environment

```{bash}
pip install dbx
```

```{bash}
export DATABRICKS_AAD_TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq -r .accessToken)
export DATABRICKS_HOST=https://adb-7393756451346106.6.azuredatabricks.net/
databricks configure --jobs-api-version 2.1 --host $DATABRICKS_HOST --aad-token
```

Check it worked by listing the clusters

```{bash}
databricks clusters list
```

Add a repo/folder on databricks to sync to...

Open [databricks](https://adb-7393756451346106.6.azuredatabricks.net/) in a web browser.
Navigate to Repos in the side panel, within you're user directory right click and add repo.
Uncheck _Add repo by cloning a git repository_ and instead enter a name for your repo e.g. _elmo-geo-dev_. The _-dev_ here is to differentiate from _elmo-geo_ which you might have cloned directly from github.

Sync your local files to the destination you just created...

```{bash}
dbx sync repo -d elmo-geo-dev
```

You should now be able to make edits to a file and see the changes sync to databricks!

Add the folliwing lines to the top of your entry notebook to enable hot reloading

```{bash}
%load_ext autoreload
%autoreload 2
```
