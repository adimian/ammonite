export PYTHONPATH=ammonite
find . -name "*.pyc" -delete && py.test $@
