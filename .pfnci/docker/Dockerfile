FROM ubuntu:20.04

LABEL maintainer="tianqi@preferred.jp"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata && \
    apt-get install -y --no-install-recommends libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev python-openssl ca-certificates zip tzdata libsqlite3-dev \
    gcc g++ cmake make libffi-dev patch wget && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

ADD pyenv.tar.gz /root/
RUN ls /root
RUN mv /root/pyenv-2.2.0 /root/.pyenv
ENV PYENV_ROOT /root/.pyenv

COPY install-pyenv.sh /tmp/install-pyenv.sh
RUN bash -c /tmp/install-pyenv.sh

COPY install-python.sh /tmp/install-python.sh

# RUN /tmp/install-python.sh "3.6.15"
RUN /tmp/install-python.sh "3.7.12"
RUN /tmp/install-python.sh "3.8.12"
RUN /tmp/install-python.sh "3.9.7"
RUN /tmp/install-python.sh "3.10.0"

# install tox in the newest python
COPY install-tox.sh /tmp/install-tox.sh
RUN bash -c /tmp/install-tox.sh
