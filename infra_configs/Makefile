SPARK_JARS_BASE_IMAGE ?= spark-jars-base
SPARK_JARS_BASE_TAG ?= latest
SPARK_IMAGE ?= spark-custom
SPARK_TAG ?= latest


.PHONY: all
all: build-spark-jars-base build-spark

.PHONY: build-spark-jars-base
build-spark-jars-base:
	docker build \
		-t $(JARS_BASE_IMAGE):$(JARS_BASE_TAG) \
		-f /spark-jars-base/Dockerfile

.PHONY: build-spark
build-spark: 
	docker build $(DOCKER_BUILD_OPTS) \
		-t $(SPARK_IMAGE):$(SPARK_TAG) \
		-f /spark/Dockerfile
