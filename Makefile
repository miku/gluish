clean:
	find . -name "*.pyc" -exec rm -rf {} \;
	rm -rf build/ dist/ gluish.egg-info/

coverage:
	nosetests --with-coverage --cover-package=gluish