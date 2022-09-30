#!/usr/bin/env bash
set -eux

export PYENV_ROOT=$HOME/.pyenv

export PATH=${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}
pyenv global 3.7.13 3.8.13
python -m pip install --upgrade pip
pip install tox
pip install -e .[test]
tox -e py37,py38
