#!/bin/bash
set -ex

eval $(minikube docker-env)
docker build -t geoanalysis-image:latest .