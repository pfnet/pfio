#!/usr/bin/env bash
set -eux

source /root/.bash_docker
pyenv global 3.8.12 3.9.7 3.10.0
tox -e py38,py39,py310 && :
tox_status=$?

# test doc, needs python >= 3.6
pyenv global 3.9.7
pip install .[doc]
cd docs && make html && :
sphinx_status=$?

echo "tox_status=${tox_status}"
echo "sphinx_status=${sphinx_status}"
exit $((tox_status || sphinx_status))
