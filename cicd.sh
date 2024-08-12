#!/bin/bash
set -ex

mvn clean package

eval $(minikube docker-env)
docker build -t spark-job-image:latest .
