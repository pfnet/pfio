.PHONY: build

pyenv.tar.gz:
	wget https://github.com/pyenv/pyenv/archive/refs/tags/v2.1.0.tar.gz -O $@

build: pyenv.tar.gz
	@docker build -t asia.gcr.io/pfn-public-ci/pfio:latest .

push:
	@docker push asia.gcr.io/pfn-public-ci/pfio:latest
