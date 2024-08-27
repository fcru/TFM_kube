#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t ingest-estat-estacions-image:latest .

kubectl apply -f cronjob-fetch-estat-estacions-data.yaml
