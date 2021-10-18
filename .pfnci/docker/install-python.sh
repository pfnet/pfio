#!/bin/bash

export PATH=$PYENV_ROOT/bin:$PATH
export MAKE_OPTS=-j16

source /root/.bash_docker

PYTHON_VERSION=$1

pyenv install $PYTHON_VERSION
pyenv shell $PYTHON_VERSION
pyenv global $PYTHON_VERSION
