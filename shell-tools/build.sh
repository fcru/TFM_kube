#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t shell-tools:latest .
