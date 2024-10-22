#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t streamlit-app-image:latest .