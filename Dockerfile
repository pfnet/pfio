FROM ghcr.io/astral-sh/uv:debian

RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    zip \
    && rm -rf /var/lib/apt/lists/*

COPY .python-versions .python-versions
COPY pyproject.toml pyproject.toml
RUN uv python install
COPY  run-test.sh /usr/local/bin/run-test.sh
RUN chmod +x /usr/local/bin/run-test.sh

WORKDIR /work

CMD [ "run-test.sh" ]
