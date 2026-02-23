WORKING_DIR ?= D:/Files/MAIN/DE/de_infra

SPARK_JARS_BASE_IMAGE ?= de_infra-spark-jars-base
SPARK_JARS_BASE_TAG ?= spark3.5-iceberg1.10

SPARK_IMAGE ?= de_infra-spark
SPARK_TAG ?= spark3.5-iceberg1.10

JUPYTER_IMAGE ?= de_infra-jupyter
JUPYTER_TAG ?= python3.10-spark3.5

AIRFLOW_IMAGE ?= de_infra-airflow
AIRFLOW_TAG ?= python3.10-spark3.5


.PHONY: all
all: build-spark-jars-base build-spark build-jupyter build-airflow

.PHONY: build-spark-jars-base
build-spark-jars-base:
	docker build \
		-t $(SPARK_JARS_BASE_IMAGE):$(SPARK_JARS_BASE_TAG) \
		-f ${WORKING_DIR}/infra_configs/spark-jars-base/Dockerfile \
		${WORKING_DIR}/infra_configs/spark-jars-base

.PHONY: build-spark
build-spark: 
	docker build \
		-t $(SPARK_IMAGE):$(SPARK_TAG) \
		-f ${WORKING_DIR}/infra_configs/spark/Dockerfile \
		${WORKING_DIR}/infra_configs/spark

.PHONY: build-jupyter
build-jupyter: 
	docker build \
		-t $(JUPYTER_IMAGE):$(JUPYTER_TAG) \
		-f ${WORKING_DIR}/infra_configs/jupyter/Dockerfile \
		${WORKING_DIR}/infra_configs/jupyter

.PHONY: build-airflow
build-airflow: 
	docker build \
		-t $(AIRFLOW_IMAGE):$(AIRFLOW_TAG) \
		-f ${WORKING_DIR}/infra_configs/airflow/Dockerfile \
		${WORKING_DIR}/infra_configs/airflow
