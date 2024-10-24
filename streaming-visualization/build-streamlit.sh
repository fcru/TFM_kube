#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t streamlit-streaming-app-image:latest .

kubectl apply -f cronjob-streaming-app-streamlit.yaml