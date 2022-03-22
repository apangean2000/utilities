
.PHONY: docs examples

docs:
	rm -rf docs/
	mkdir docs/
	# pdoc3
	#cp -a ./docsrc/assets/ ./docs/assets/
	pdoc3 --html --force --output-dir docs scripts/
	mv docs/scripts/* docs
	rmdir docs/scripts
	# sphinx
	cd docsrc/ && make github

test:
	pytest tests/unit/
	pytest tests/issue/
	pytest --nbval tests/notebook/

test_cov:
	pytest --cov=. tests/unit/
	pytest --cov=. --cov-append tests/issue/
	pytest --cov=. --cov-append --nbval tests/notebook/

examples:
	find ./examples -maxdepth 2 -type f -name "*.py" -execdir python {} \;

install:
	curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
	poetry install
	poetry shell

update:
	poetry update

lint:
	pre-commit run --all-files

clean:
	git rm --cached `git ls-files -i -c --exclude-from=.gitignore`

all:
	make lint
	make install
	make examples
	make docs
	make test
