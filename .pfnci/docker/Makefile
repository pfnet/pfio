.PHONY: build

# The way of path manipulation has changed since 2.0.0 - not working well.
# So far, fix the pyenv version here
pyenv.tar.gz:
	wget https://github.com/pyenv/pyenv/archive/refs/tags/1.2.27.tar.gz -O $@

build: pyenv.tar.gz
	@docker build -t asia.gcr.io/pfn-public-ci/pfio:latest .

push:
	@docker push asia.gcr.io/pfn-public-ci/pfio:latest
