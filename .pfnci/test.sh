#!/usr/bin/env sh
set -eux

gcloud auth configure-docker

docker run --interactive --rm \
       --volume "$(pwd):/repo/" --workdir /repo/ \
       tianqixu/chainerio:ubuntu \
       bash -ex << EOD
source /root/.bashrc
pyenv global ${PYTHON}
pip install --user -e .[test]
export PATH=/root/.local/bin:$PATH
pytest tests -s -v
flake8 chainerio
flake8 tests
autopep8 -r chainerio tests --diff | tee check_autopep8
test ! -s check_autopep8
EOD
