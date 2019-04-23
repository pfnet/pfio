FROM chainer/chainer:latest-python3

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    python3-dev python3-pip zlib1g-dev make cmake g++ git \
    && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

RUN python3 -m pip install --no-cache-dir setuptools
RUN python3 -m pip install --no-cache-dir chainer \
    autopep8 flake8 mock pytest pyarrow krbticket numpy<1.16.3,>1.16.0
