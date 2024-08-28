#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t pipelines-batch-image:latest .
