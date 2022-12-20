#!/usr/bin/env bash
set -eux

export PYENV_ROOT=$HOME/.pyenv

export PATH=${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}
pyenv global 3.7.16 3.8.16 3.9.16
python -m pip install --upgrade pip
pip install tox
tox -r -e py37,py38,py39
