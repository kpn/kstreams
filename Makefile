# Docker Compse options
service=kafka
partitions=1
replication-factor=1
cleanup-policy=delete

doc:
	cd docs && make html
	@echo "open file://`pwd`/docs/_build/html   /index.html"

clean:
	find . -name __pycache__ |xargs rm -rf
	find . -type f -name '*.py[co]' -delete
	find . -type f -name '*~' -delete
	find . -type f -name '.*~' -delete
	find . -type f -name '@*' -delete
	find . -type f -name '#*#' -delete
	find . -type f -name '*.orig' -delete
	find . -type f -name '*.rej' -delete
	rm -f .coverage
	rm -rf coverage
	rm -rf docs/_build
	rm -rf .tox
	rm -rf venv
	find . -name '*.py[cod]' -delete
	rm -rf *.egg-info build dist
	rm -rf coverage.xml .coverage junit.xml

venv:
	poetry install

# Mimic test run like Jenkins will do inside the docker container
docker-kafka:=docker run --rm -u $(shell id -u ${USER}):$(shell id -g ${USER}) --name 'docker-kafka' \
	-v $(shell pwd):$(shell pwd) -w $(shell pwd) \
	-e HOME=$(shell pwd) \
	kafka:latest

# Docker Compose
bash:
	docker-compose run --user=$(shell id -u) ${service} bash

restart:
	docker-compose restart ${service}

run:
	docker-compose up

logs:
	docker-compose logs

# Removes old containers, free's up some space
remove:
	# Try this if this fails: docker rm -f $(docker ps -a -q)
	docker-compose rm --force -v

remove-network:
	docker network rm py-streams_default || true

stop-kafka-cluster:
	docker-compose stop
	@$(MAKE) remove
	@$(MAKE) remove-network

kafka-cluster: run

# Kafka related
list-topics:
	docker-compose exec kafka kafka-topics --list --zookeeper zookeeper:32181

create-topic:
	docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor ${replication-factor} --partitions ${partitions} --config cleanup.policy=${cleanup-policy} --topic ${topic-name}

.PHONY: create-dev-topics
create-dev-topics:
	@$(MAKE) create-topic topic-name="dev-kpn-des--kstreams"
	@$(MAKE) create-topic topic-name="dev-kpn-des--hello-world"

# Development
test:
	./scripts/test

lint:
	./scripts/lint

install:
	./scripts/install

.PHONY: all test clean

bump:
	# scenario: deployment
	cz bump --yes --changelog

bump/prerelease:
	cz bump --prerelease rc
