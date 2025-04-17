docker ?= docker
image ?= public.ecr.aws/eodh/billing-collector
version ?= 0.1.0
port ?= 8000

.SILENT:
MAKEFLAGS += --no-print-directory

.PHONY: container-build
container-build:
	$(docker) build -t $(image):$(version) .

.PHONY: container-run
container-run:
	$(docker) run --network host --rm \
		-p $(port):8000 $(image):$(version)

.PHONY: container-push
container-push:
	$(docker) push $(image):$(version)

.PHONY: publish
publish: container-build container-push

.PHONY: check
check:
	ruff check . --select I --fix

.PHONY: fmt
fmt:
	ruff format .

.PHONY: test
test:
	python -m pytest -v .