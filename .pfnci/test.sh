#!/usr/bin/env bash
set -eux

source /root/.bash_docker
pyenv global 3.5.2 3.6.8 3.7.2
tox && :
tox_status=$?

# test doc, needs python >= 3.6
pyenv global 3.7.2
pip install .[doc]
cd docs && make html && :
sphinx_status=$?
echo "tox_status=${tox_status}"
echo "sphinx_status=${sphinx_status}"

exit $((tox_status || sphinx_status))
