FROM ubuntu:18.04

LABEL maintainer="tianqi@preferred.jp"

RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev wget python-openssl git ca-certificates && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ cmake make libffi-dev

COPY install.sh /tmp/install.sh

RUN bash -c /tmp/install.sh

RUN apt-get remove -y gcc g++ cmake libreadline-dev python-openssl && apt-get -y autoremove
