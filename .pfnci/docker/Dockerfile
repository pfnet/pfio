FROM ubuntu:20.04

LABEL maintainer="kota@preferred.jp"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata && \
    apt-get install -y --no-install-recommends libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev python-openssl ca-certificates zip tzdata libsqlite3-dev \
    gcc g++ cmake make libffi-dev patch wget liblzma-dev

ADD pyenv.tar.gz /root/
# RUN ls /root
RUN mv /root/pyenv-2.3.9 /root/.pyenv
ENV PYENV_ROOT /root/.pyenv

# COPY install-pyenv.sh /tmp/install-pyenv.sh
# RUN bash /tmp/install-pyenv.sh

RUN $PYENV_ROOT/plugins/python-build/install.sh

ENV PATH ${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}
ENV MAKE_OPTS "-j12"

RUN pyenv install 3.11.1
RUN pyenv install 3.10.9
RUN pyenv install 3.7.16
RUN pyenv install 3.8.16
RUN pyenv install 3.9.16
RUN pyenv global 3.11.1

# RUN pip install tox
