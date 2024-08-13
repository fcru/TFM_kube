#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t python-code-image:latest .