VENV=venv

.PHONY: help test lint install ingest run

help:
	@echo "Targets will be implemented during development."

test:
	$(VENV)\Scripts\pytest.exe

lint:
	$(VENV)\Scripts\ruff.exe check .

format:
	$(VENV)\Scripts\ruff.exe format .

fix:
	$(VENV)\Scripts\ruff.exe check . --fix

docs:
	$(VENV)\Scripts\ruff.exe check . --select D

typecheck:
	$(VENV)\Scripts\pyright.exe

check:
	$(VENV)\Scripts\ruff.exe check .
	$(VENV)\Scripts\pyright.exe
	$(VENV)\Scripts\pytest.exe

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