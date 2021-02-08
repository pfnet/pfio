#!/bin/bash

function install_py()
{
    PYTHON_VERSION=$1
    PYENV_ROOT=$2
    pyenv install $PYTHON_VERSION
    pyenv shell $PYTHON_VERSION
    pyenv global $PYTHON_VERSION
}

python_versions=('3.6.12' '3.7.9' '3.8.7' '3.9.1')
PYENV_ROOT=/root/.pyenv

rm -rf $PYENV_ROOT
git clone git://github.com/pyenv/pyenv.git $PYENV_ROOT
export PATH=$PYENV_ROOT/bin:$PATH
export MAKE_OPTS=-j16
$PYENV_ROOT/plugins/python-build/install.sh
eval "$(pyenv init -)"
echo "export PATH=$PYENV_ROOT/bin:$PATH" >> /root/.bash_docker
echo "eval '$(pyenv init -)'" >> /root/.bash_docker

for version in "${python_versions[@]}"
do
    install_py $version $PYENV_ROOT
done

# install tox in the newest python
pip install tox
