#!/usr/bin/env sh

set -eux

cd "$(dirname "${BASH_SOURCE}")"/..

gcloud auth configure-docker
# Prepare docker args.
docker run  --rm --volume="$(pwd):/repo/" --workdir="/repo/" \
    "asia-northeast1-docker.pkg.dev/pfn-artifactregistry/public-ci-pfio/pfio:latest" \
    bash .pfnci/test-old.sh
