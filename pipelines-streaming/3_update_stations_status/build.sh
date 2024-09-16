#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t stream-update-status-image:latest .

kubectl apply -f cronjob-stream-update-status.yaml
