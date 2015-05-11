export PYTHONPATH=ammonite
find . -name "*.pyc" -delete && py.test --cov-config .coveragerc --cov-report html --cov ammonite
