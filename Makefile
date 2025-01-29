CURRENT_VERSION=$(shell cat ./server/__init__.py | cut -d "'" -f 2)
DOCKER_IMAGE_NAME=dataesr/projects_partners_consolidation
GHCR_IMAGE_NAME=ghcr.io/$(DOCKER_IMAGE_NAME)

test: unit

unit: start
	@echo Running unit tests...
	APP_ENV=test venv/bin/python -m pytest
	@echo End of unit tests

start: stop
	@echo Affiliation matcher starting...
	docker compose up -d
	@echo Affiliation matcher started http://localhost:5000

stop:
	@echo Affiliation matcher stopping...
	docker compose down
	@echo Affiliation matcher stopped

install:
	@echo Installing dependencies...
	pip install -r requirements.txt
	@echo End of dependencies installation

client-build:
	@echo Building client files
	cd project/client && npm install && npm run build

docker-build:
	@echo Building a new docker image
	docker build -t $(GHCR_IMAGE_NAME):$(CURRENT_VERSION) -t $(GHCR_IMAGE_NAME):latest .
	@echo Docker image built

docker-push:
	@echo Pushing a new docker image
	docker push -a $(GHCR_IMAGE_NAME)
	@echo Docker image pushed

release:
	echo "__version__ = '$(VERSION)'" > project/__init__.py
	cd project/client && npm version $(VERSION)
	make client-build
	git commit -am '[release] version $(VERSION)'
	git tag $(VERSION)
	@echo If everything is OK, you can push with tags i.e. git push origin master --tags