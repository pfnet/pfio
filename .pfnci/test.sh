#!/usr/bin/env bash
set -eux

source /root/.bash_docker
pyenv global 3.5.7 3.6.8 3.7.2
tox && :
tox_status=$?
pip install .[doc]
cd docs && make html && :
sphinx_status=$?
echo "tox_status=${tox_status}"
echo "sphinx_status=${sphinx_status}"

exit $((tox_status || sphinx_status))
