#!/usr/bin/env bash
set -eux

source /root/.bash_docker
pyenv global 3.6.15 #3.7.12 3.8.12 3.9.7 3.10.0
pip install tox
pip install https://files.pythonhosted.org/packages/72/b5/01d4730395cf27ec679debfeab60efaafebd10177ff5b04eef5d530a6bf8/pyarrow-6.0.1-cp36-cp36m-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
pip install -e .[test]
tox -e py36
