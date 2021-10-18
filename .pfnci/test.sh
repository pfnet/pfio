#!/usr/bin/env bash
set -eux

source /root/.bash_docker
pyenv global 3.6.13 3.7.10 3.8.9 3.9.4
tox && :
tox_status=$?

# test doc, needs python >= 3.6
pyenv global 3.9.4
pip install .[doc]
cd docs && make html && :
sphinx_status=$?

echo "tox_status=${tox_status}"
echo "sphinx_status=${sphinx_status}"
exit $((tox_status || sphinx_status))
