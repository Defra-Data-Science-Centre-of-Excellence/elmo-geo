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
	pipx install "ruff<0.2" pip-tools
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

install_spell:
	echo 'Using: https://deb.nodesource.com/ to install nodejs, npm, and cspell.'
	v=20
	curl -sL https://deb.nodesource.com/setup_$v.x | sudo -E bash -
	sudo apt-get install -y nodejs npm
	sudo npm install -g cspell@latest
	echo 'Installing: codespell.'
	pipx install codespell

cspell:
	cspell . --words-only -u --quiet | tr '[:upper:]' '[:lower:]' | sort | uniq > data/dictionary-output.txt

codespell:
	codespell . -wsfL arange,hefer,hist,humber,jupyter,ons -i2
