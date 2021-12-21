#!/usr/bin/env bash
set -eux

source /root/.bash_docker
pyenv global 3.6.15 #3.7.12 3.8.12 3.9.7 3.10.0
python -m pip install --upgrade pip
pip install tox
pip install -e .[test]
tox -e py36
