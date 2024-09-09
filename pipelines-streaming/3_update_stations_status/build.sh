#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t stream-estat-estacions-image:latest .

kubectl apply -f cronjob-stream-estat-estacions-data.yaml
