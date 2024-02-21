dbx:
	export DATABRICKS_AAD_TOKEN=$$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d | jq -r .accessToken)
	export DATABRICKS_HOST=https://adb-7393756451346106.6.azuredatabricks.net/
	databricks configure --jobs-api-version 2.1 --host $$DATABRICKS_HOST --aad-token
	dbx sync repo -d elmo-geo-dev

freeze:
	pip-compile -qU --all-extras

fmt:
	ruff check . --fix
	ruff format .

verify:
	ruff check .
	#ruff format . --check
	PYTHONDONTWRITEBYTECODE=1 pytest -m without_cluster  -v -p no:cacheprovider .

clean:
	ruff clean
	py3clean .
	rm -r \
		.pytest_cache/ \
		build/ \
		*.egg-info \
		2> /dev/null || true
	clear
