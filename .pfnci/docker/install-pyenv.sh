#!/bin/bash

export PATH=$PYENV_ROOT/bin:$PATH
$PYENV_ROOT/plugins/python-build/install.sh

echo "export PATH=$PYENV_ROOT/bin:$PATH" >> /root/.bash_docker
echo "eval '$(pyenv init -)'" >> /root/.bash_docker
