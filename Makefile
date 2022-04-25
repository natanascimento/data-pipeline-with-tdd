SHELL := /bin/bash
DATA_OUT_PATH := $(shell pwd)
CONTAINER_ID := $(shell docker ps -qf "name=spark_experience")

.PHONY: help build start stop
.DEFAULT: help

help:
	@echo "make build"
	@echo "          Builds docker image"
	@echo "----------"
	@echo "make start"
	@echo "          Mounts notebooks volume and starts docker compose services"
	@echo "----------"
	@echo "make stop"
	@echo "          Stops docker compose services"
	@echo "----------"

build: 
	docker build -t spark_experience:v.3.1.2-1.0.0 .

start:
	docker-compose up -d

stop:
	docker-compose down

restart: stop start

logs:
	docker logs -f ${CONTAINER_ID}

exec: 
	docker exec -it ${CONTAINER_ID} /bin/bash