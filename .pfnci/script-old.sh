#!/usr/bin/env sh

set -eux

cd "$(dirname "${BASH_SOURCE}")"/..

gcloud auth configure-docker
# Prepare docker args.
docker run  --rm --volume="$(pwd):/repo/" --workdir="/repo/" \
    "asia.gcr.io/pfn-public-ci/pfio:latest" \
    bash .pfnci/test-old.sh $1
