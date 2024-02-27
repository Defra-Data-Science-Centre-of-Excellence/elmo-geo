clean:
	ruff clean
	py3clean .
	rm -r \
		.pytest_cache/ \
		build/ \
		*.egg-info \
		2> /dev/null || true
	clear

install:
	python -m pip install --upgrade pip setuptools wheel
	pip install -r requirements.txt

fmt:
	ruff check . --fix
	ruff format .

freeze:
	pip-compile -qU --all-extras

verify:
	ruff check .
	ruff format . --check
	pytest .

verify_dbr:
	ruff check .
	ruff format . --check
	pip-compile -q --all-extras
	PYTHONDONTWRITEBYTECODE=1 pytest .
