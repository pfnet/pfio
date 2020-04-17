FROM ubuntu:16.04

LABEL maintainer="tianqi@preferred.jp"

RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev wget python-openssl git ca-certificates zip && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ cmake make libffi-dev patch

COPY install.sh /tmp/install.sh

RUN bash -c /tmp/install.sh
