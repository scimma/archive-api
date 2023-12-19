##
## The "push" target assumes that AWS credentials
## have been configured.
##
CNT_NAME := scimma/archive-api
REGION   := us-west-2
AWSREG   := 585193511743.dkr.ecr.us-west-2.amazonaws.com

TAG      := $(shell git log -1 --pretty=%H || echo MISSING )
CNT_IMG  := $(CNT_NAME):$(TAG)
CNT_LTST := $(CNT_NAME):latest

.PHONY: set-release-tags push test clean all container

all: container

container: Dockerfile
	docker build -f $< -t $(CNT_IMG) .
	docker tag $(CNT_IMG) $(CNT_LTST)

set-release-tags:
	@$(eval TAG_SUFFIX := $(shell if [ ! -z "$$(git status --porcelain)" ]; then echo '-modified'; fi))
	@$(eval RELEASE_TAG := $(shell git describe --tags --abbrev=0 | sed 's/^[^0-9]*//')"$(TAG_SUFFIX)")
	@echo RELEASE_TAG =  $(RELEASE_TAG)
	@$(eval MAJOR_TAG   := $(shell echo $(RELEASE_TAG) | awk -F. '{print $$1}'))
	@echo MAJOR_TAG = $(MAJOR_TAG)
	@$(eval MINOR_TAG   := $(shell echo $(RELEASE_TAG) | awk -F. '{print $$2}'))
	@echo MINOR_TAG = $(MINOR_TAG)

push: set-release-tags
	@(echo $(RELEASE_TAG) | grep '^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]$$' > /dev/null ) || (echo Bad release tag: $(RELEASE_TAG) && exit 1)
	/usr/local/bin/aws ecr get-login-password | docker login --username AWS --password-stdin $(AWSREG)
	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME):$(RELEASE_TAG)
	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG)
	docker tag $(CNT_IMG) $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG).$(MINOR_TAG)
	docker push $(AWSREG)/$(CNT_NAME):$(RELEASE_TAG)
	docker push $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG)
	docker push $(AWSREG)/$(CNT_NAME):$(MAJOR_TAG).$(MINOR_TAG)
	rm -f $(HOME)/.docker/config.json