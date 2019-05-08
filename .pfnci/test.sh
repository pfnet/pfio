#!/usr/bin/env sh
set -eux

gcloud auth configure-docker

docker run --interactive --rm \
       --volume "$(pwd):/repo/" --workdir /repo/ \
       chainer/chainerio:latest \
       bash << EOD
source /root/.bashrc
pyenv global 3.5.7 3.6.8 3.7.2
tox
EOD
