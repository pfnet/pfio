#!/usr/bin/env bash
set -eux

source /root/.bash_docker
pyenv global 3.6.15 3.7.12 #3.8.12 3.9.7 3.10.0
tox -e py36
