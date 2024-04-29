# elmo-geo
Part of [ELMO][elmo], separated to work on [DASH][dash] for large scale geospatial analysis.


## Getting Started
Use DASH's [onbording guide][dash_onboarding] to get access, it requires you to submit a MyIT ticket.  For elmo-geo you will need extra permissions.
|   |   |
|---|---|
Workspace | PRDDAPINFDB02<b>406</b>
User Group | UC_DASH_ELM_Users

Seek help from FCP Business Admins; Amy Cairns, Ed Burrows, Andrew West.


## Workflow
[DASH Playbook][dash_playbook]

### Databricks, Clusters, Notebooks, and Environments

### Medallion Architecture
|   |   |   |
|---|---|---|
bronze | As it comes | `dbfs:/mnt/base`<br>`dbfs:/mnt/lab/restricted/ELM-Project/bronze`<br>~`/Volumes/prd_dash_lab/fcp_restricted/bronze`~
silver | ready to use | `dbfs:/mnt/lab/restricted/ELM-Project/silver`<br>~`/Volumes/prd_dash_lab/fcp_restricted/silver`~
gold | ready to output | `dbfs:/mnt/lab/restricted/ELM-Project/gold`<br>~`/Volumes/prd_dash_lab/fcp_restricted/gold`~

Unity Catalog (`/Volumes/`) is in testing.

### Testing


## Projects
| Status | Project | Description |
| ------ | ------- | ----------- |
| | Ingestion |
| | Parcel Join]() |
| | Sylvan]() |
| | Boundary Use]() |
| | [Bare Soils]() |
| | [Others]() | Business Info, Segementation,



<details><summary><h2>Setup</h2></summary>

### Install requirements

On your local development machine...

Clone the repo:

```{bash}
git clone git@github.com:Defra-Data-Science-Centre-of-Excellence/elmo-geo.git
```

Install azure cli:

```{bash}
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

Install databricks and dbx in your python virtual environment:

```{bash}
pip install dbx
```

### Authenticating to databricks with Azure CLI

Login to Azure CLI:

```{bash}
az login --tenant bce3d7d1-cbbd-481e-8c81-eaecfc38b551 --use-device-code --allow-no-subscriptions
```

Login to databricks by authenticating with ADD:

```{bash}
DATABRICKS_AAD_TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq -r .accessToken) \
DATABRICKS_HOST=https://adb-7393756451346106.6.azuredatabricks.net/ \
databricks configure --jobs-api-version 2.1 --host $DATABRICKS_HOST --aad-token
```

Check it worked by listing the clusters:

```{bash}
databricks clusters list
```

### Using _dbx sync_

Note: Databricks have a VS Code extension that would replace the need for dbx.
This is in preview and does not currently support authentication via ADD and so cannot be used without the ability to generate a Databricks auth token.

Add a repo/folder on databricks to sync to:

Open [databricks](https://adb-7393756451346106.6.azuredatabricks.net/) in a web browser.
Navigate to Repos in the side panel, within you're user directory right click and add repo.
Uncheck _Add repo by cloning a git repository_ and instead enter a name for your repo e.g. _elmo-geo-dev_. The _-dev_ here is to differentiate from _elmo-geo_ which you might have cloned directly from github.

Sync your local files to the destination you just created:

```{bash}
dbx sync repo -d elmo-geo-dev
```

You should now be able to make edits to a file and see the changes sync to databricks!

Add the following lines to the top of your entry notebook to enable hot reloading

```{bash}
%load_ext autoreload
%autoreload 2
```

When you startup your machine again you will need to rerun the databricks authentication steps. These tasks been added to the makefile to simplify things. Just run `make dbx`!

## Installing dependencies

To install the package and all required development dependencies:

```{bash}
pip install -e .[dev]
```

</details>

<details><summary><h2>Testing</h2></summary>

There are currently two ways to run the tests located in the ./tests/ directory.

1. Running the "./tests/Run Tests" notebook.

Because the notebook can be connected to a Databricks compute resource (i.e. a cluster) this notebook runs all tests, including those that require access to an active spark session and dbutils.

2. Runing "make verify" or "make test" from the command line.

This only runs tests that have been marked as "without_cluster" as when running from the command line some features of Databricks comute resources are not available. If these features can be effectively mocked the "without_cluster" marker may not be required. Separating out these tests enables some test to be run as part of a continuous integration process.

For more information on using pytest with Databricks see https://docs.databricks.com/en/notebooks/testing.html

</details>


[elmo]: https://github.com/Defra-Data-Science-Centre-of-Excellence/elm_modelling_strategy/
[dash]: https://defra.sharepoint.com/sites/Community448/SitePages/Welcome-to-the-Data-Science-Centre-of-Excellence.aspx
[dash_onboarding]: https://defra.sharepoint.com/sites/Community448/SitePages/Onboarding.aspx
[dash_playbook]: https://github.com/Defra-Data-Science-Centre-of-Excellence/DASH-Playbook
[dash_myit]: https://defragroup.service-now.com/esc?id=sc_cat_item_guide&table=sc_cat_item&sys_id=025906fb1b99f190848b8594e34bcb67
