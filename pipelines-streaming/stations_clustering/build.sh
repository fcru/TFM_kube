#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t clustering-stations-image:latest .

kubectl apply -f cronjob-stations-clusters.yaml
