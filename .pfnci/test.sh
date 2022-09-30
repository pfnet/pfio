#!/usr/bin/env bash
set -eux

export PYENV_ROOT=$HOME/.pyenv

export PATH=${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}
pyenv global 3.9.13 3.10.6
tox -e py39,py310 && :
tox_status=$?

# test doc, needs python >= 3.6
pyenv global 3.10.6
pip install .[doc]
cd docs && make html && :
sphinx_status=$?

echo "tox_status=${tox_status}"
echo "sphinx_status=${sphinx_status}"
exit $((tox_status || sphinx_status))
