dbx:
	export DATABRICKS_AAD_TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq -r .accessToken)
	export DATABRICKS_HOST=https://adb-7393756451346106.6.azuredatabricks.net/
	databricks configure --jobs-api-version 2.1 --host $DATABRICKS_HOST --aad-token
	dbx sync repo -d elmo-geo-dev

freeze:
	pip freeze --exclude-editable | grep -v "file:///" > requirements.txt

fmt:
	isort elmo_geo
	black elmo_geo

verify:
	isort --check-only elmo_geo
	black --diff --check elmo_geo
	flake8 elmo_geo
	pytest .

clean:
	rm -r *.egg-info 2> /dev/null || true
	py3clean .