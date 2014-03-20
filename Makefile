clean:
	find . -name "*.pyc" -exec rm -rf {} \;
	rm -rf build/ dist/ gluish.egg-info/