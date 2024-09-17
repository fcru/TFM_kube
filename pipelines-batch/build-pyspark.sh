#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t pyspark-code-image:latest .
