#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t landing-image:latest .
