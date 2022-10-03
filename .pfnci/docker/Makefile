.PHONY: build

pyenv.tar.gz:
	wget https://github.com/pyenv/pyenv/archive/refs/tags/v2.3.4.tar.gz -O $@

build: pyenv.tar.gz
	@docker build -t asia-northeast1-docker.pkg.dev/pfn-artifactregistry/public-ci-pfio/pfio:latest .

push:
	@docker push asia-northeast1-docker.pkg.dev/pfn-artifactregistry/public-ci-pfio/pfio:latest
