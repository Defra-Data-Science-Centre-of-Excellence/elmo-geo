clean:
	ruff clean
	py3clean .
	rm -r \
		.pytest_cache/ \
		build/ \
		*.egg-info \
		2> /dev/null || true
	git branch --merged | grep -v \* | xargs git branch -D
	clear

install:
	python -m pip install --upgrade pip setuptools wheel
	pipx install "ruff<0.2" pip-tools typos
	pip install -r requirements.txt

fmt:
	ruff check . --fix
	ruff format .

freeze:
	pip-compile -q --all-extras --no-strip-extras

verify_gh:
	ruff check .
	ruff format . --check
	pytest . -m "not dbr"

verify:
	ruff check .
	ruff format . --check
	PYTHONDONTWRITEBYTECODE=1 pytest .

latest_clusters_log:
	find /dbfs/cluster-logs/ -type f -name "*.stderr.log" | awk -F/ '{print $NF, $0}' | sort | awk '{print $2}' | tail -n1 | xargs cat

spell-check:
	typos . --format=brief --sort

spell-correct:
	typos . --format=brief --sort -w
