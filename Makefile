dist:
	python setup.py sdist

clean:
	find . -name "*.pyc" -exec rm -rf {} \;
	rm -rf build/ dist/ gluish.egg-info/ .tox/

coverage:
	nosetests --with-coverage --cover-package=gluish

imports:
	isort -rc --atomic .

pylint:
	pylint siskin
