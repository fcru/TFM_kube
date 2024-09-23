#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t stations-assignment-image:latest .

kubectl apply -f cronjob-stations-assignment.yaml
