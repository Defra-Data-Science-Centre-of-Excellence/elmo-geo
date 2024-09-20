# elmo-geo
This repository is an companion to [ELMO][elmo]. It has been separated to work on [DASH][dash] for large scale geospatial analysis.  Primarily, the outputs from elmo-geo inform the missing [eligibility criteria][elig] for ELMO.

| Status | Project | Description |
| ------ | ------- | ----------- |
| :gear: | Ingestion | [readme](notebooks/ingestion/readme.md)
| :gear: | Sylvan |
| :gear: | Boundary |
| :gear: | Bare Soils |

Table of Projects: :negative_squared_cross_mark: Planned, :gear: Ongoing, :ballot_box_with_check: Complete.



## Getting Started
Use DASH's [onboarding guide][dash_onboarding] to get access, it requires you to submit a MyIT ticket.  For elmo-geo you will need extra permissions.
|   |   |
|---|---|
Workspace | PRDDAPINFDB02<b>406</b>
User Group | UC_DASH_ELM_Users

Seek help from FCP Business Admins; Amy Cairns, Ed Burrows, Andrew West.


### Workflow
Use the [DASH Playbook][dash_playbook] as a guide on how to use Databricks.  

You will be using Databricks Notebooks, similar to Jupyter, rather than RStudio or Spyder.


#### Clusters and Environments
These are managed by your Business Admins, please contact them if you need support.
If you want to test a package in a Databricks Notebook run `%pip install <package>`.
For more permanence create a PR adding the package to `./pyproject.toml`.


#### Testing and Contributing
To contribute your work will be validated using Ruff and pytest.
- Please add unit tests, in `./tests` to ensure your work is doing as you expect.
- Use a shell cell to `%sh make verify` in databricks.  
  _I recommend creating a notebook `./.test` which isn't committed to github and is in the root directory._  
  _You can also run `%sh make fmt` on modules, or `%sh make clean` from here._  
![test_notebook](https://github.com/Defra-Data-Science-Centre-of-Excellence/elmo-geo/assets/81236667/3277e5a3-d632-4fc5-a758-e929dffc4be2)
- Before submitting a PR lint your work by opening a GitHub codespace, changing to your branch, and running `make verify_gh` to format+lint+verify your contributions.  
  _`pytest.marks.dbr` are avoided by `make verify_gh`, as they can only be run on Databricks, e.g. those that require spark or data._
  ~_For more information on using pytest with Databricks see https://docs.databricks.com/en/notebooks/testing.html_  
![codespaces](https://github.com/Defra-Data-Science-Centre-of-Excellence/elmo-geo/assets/81236667/9e8493e8-3712-4236-b2ad-f1bce0156837)


### Medallion Architecture
[Medallion Architecture][databricks_medallion] uses Bronze/Silver/Gold to organise datasets that are; Bronze have come from a data provider ("as is"), Silver have been modified for easier analysis ("ready to use"), Gold are analysis outputs suitable for stakeholders ("ready to output").
|   |   |   |
|---|---|---|
bronze | As it comes | `dbfs:/mnt/base`<br>`dbfs:/mnt/lab/restricted/ELM-Project/bronze`<br>~`/Volumes/prd_dash_lab/fcp_restricted/bronze`~
silver | ready to use | `dbfs:/mnt/lab/restricted/ELM-Project/silver`<br>~`/Volumes/prd_dash_lab/fcp_restricted/silver`~
gold | ready to output | `dbfs:/mnt/lab/restricted/ELM-Project/gold`<br>~`/Volumes/prd_dash_lab/fcp_restricted/gold`~

Unity Catalog (`/Volumes/`) is in testing.



[elmo]: https://github.com/Defra-Data-Science-Centre-of-Excellence/elm_modelling_strategy/
[dash]: https://defra.sharepoint.com/sites/Community448/SitePages/Welcome-to-the-Data-Science-Centre-of-Excellence.aspx
[elig]: https://animated-system-bf6fb80a.pages.github.io/Reference/elmo/eligibility/criteria.html
[dash_onboarding]: https://defra.sharepoint.com/sites/Community448/SitePages/Onboarding.aspx
[dash_playbook]: https://github.com/Defra-Data-Science-Centre-of-Excellence/DASH-Playbook
[dash_myit]: https://defragroup.service-now.com/esc?id=sc_cat_item_guide&table=sc_cat_item&sys_id=025906fb1b99f190848b8594e34bcb67
[databricks_medallion]: https://www.databricks.com/glossary/medallion-architecture
