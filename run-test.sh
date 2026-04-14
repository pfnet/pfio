#!/bin/sh

uv sync
uv run tox --skip-env doc
