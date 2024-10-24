#!/bin/bash
set -ex

eval $(minikube docker-env)
<<<<<<<< HEAD:pipelines-batch/prevision_demanda/build-batch-pipelines.sh
docker build -t pipelines-batch-image:latest .
========
docker build -t exploitation-image:latest .
>>>>>>>> PRE_DemandaFusion:pipelines-batch/expansion_strategy/exploitation_zone/build-expansion-exploitation.sh
