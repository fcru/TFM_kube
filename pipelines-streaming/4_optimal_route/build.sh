#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t optimal-route-image:latest .

kubectl apply -f cronjob-optimal-route.yaml
