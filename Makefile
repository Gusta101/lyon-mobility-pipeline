VENV=venv

.PHONY: help test lint install ingest run

help:
	@echo "Targets will be implemented during development."

test:
	@echo "TODO: run tests"

lint:
	@echo "TODO: run lint"

venv:
	@if not exist $(VENV) py -m venv $(VENV)

install: venv
	$(VENV)\Scripts\python.exe -m pip install --upgrade pip
	$(VENV)\Scripts\python.exe -m pip install -e .
	$(VENV)\Scripts\python.exe -m pip install -r requirements.txt

ingest:
	$(VENV)\Scripts\python.exe .\medallion\bronze\jobs\ingest_gbfs.py

setup: install

run: ingest