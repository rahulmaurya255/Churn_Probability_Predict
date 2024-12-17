

NS ?= asia.gcr.io/jp-k8s-internal
VERSION ?= latest

IMAGE_NAME ?= beckn-etl
CONTAINER_NAME ?= beckn-etl
CONTAINER_INSTANCE ?= default

SOURCE_COMMIT := $(shell git rev-parse HEAD)

.PHONY: build aws-auth push shell run start stop rm release

build: Dockerfile
	$(info Building $(IMAGE_NAME):$(VERSION) / git-head: $(SOURCE_COMMIT))
	docker build -t $(IMAGE_NAME):$(VERSION) -f Dockerfile --build-arg "SOURCE_COMMIT=$(SOURCE_COMMIT)" .

aws-auth:
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com

push:
	docker tag $(IMAGE_NAME):$(VERSION) $(NS)/$(IMAGE_NAME):$(VERSION)
	docker push $(NS)/$(IMAGE_NAME):$(VERSION)

default: build
