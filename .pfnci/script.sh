#!/usr/bin/env sh

set -eux

cd "$(dirname "${BASH_SOURCE}")"/..

################################################################################
# Main function
################################################################################
main() {
  # Initialization.
  prepare_docker &
  wait

  # Prepare docker args.
  docker_args=(docker run  --rm --volume="$(pwd):/repo/" --workdir="/repo/" )

  run "${docker_args[@]}" \
      "chainer/chainerio:latest" \
      bash .pfnci/test.sh
}

################################################################################
# Utility functions
################################################################################

# run executes a command.  If DRYRUN is enabled, run just prints the command.
run() {
  echo '+' "$@"
  if [ "${DRYRUN:-}" == '' ]; then
    "$@"
  fi
}

# prepare_docker makes docker use tmpfs to speed up.
# CAVEAT: Do not use docker during this is running.
prepare_docker() {
  # Mount tmpfs to docker's root directory to speed up.
  if [ "${CI:-}" != '' ]; then
    run service docker stop
    run mount -t tmpfs -o size=100% tmpfs /var/lib/docker
    run service docker start
  fi
  # Configure docker to pull images from gcr.io.
  run gcloud auth configure-docker
}

################################################################################
# Bootstrap
################################################################################
main "$@"
