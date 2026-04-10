BASE_IMAGE = needle-base:latest
NEEDLE_IMAGE = needle:latest
WORKER_IMAGE = needle-worker:latest
BASE_DOCKERFILE = container/base.Dockerfile
NEEDLE_DOCKERFILE = container/needle.Dockerfile
WORKER_DOCKERFILE = container/worker.Dockerfile
APPTAINER_IMAGE = container/needle.sif

# Sentinel files to track when each image was last built
.base_built: $(BASE_DOCKERFILE)
	docker build -f $(BASE_DOCKERFILE) -t $(BASE_IMAGE) .
	touch .base_built

.needle_built: $(NEEDLE_DOCKERFILE) .base_built $(shell find needle/ -type f) pyproject.toml
	docker build --network=host -f $(NEEDLE_DOCKERFILE) -t $(NEEDLE_IMAGE) .
	touch .needle_built

.worker_built: $(WORKER_DOCKERFILE)
	docker build -f $(WORKER_DOCKERFILE) -t $(WORKER_IMAGE) .
	touch .worker_built

$(APPTAINER_IMAGE): .needle_built
	apptainer build $(APPTAINER_IMAGE) docker-daemon://$(NEEDLE_IMAGE)

.PHONY: base needle worker apptainer all clean
base: .base_built
needle: .needle_built
worker: .worker_built
apptainer: $(APPTAINER_IMAGE)
all: .base_built .needle_built .worker_built $(APPTAINER_IMAGE)

clean:
	rm -f .base_built .needle_built .worker_built $(APPTAINER_IMAGE)
