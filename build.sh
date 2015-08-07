#!/bin/bash
echo "Running build for ammonite"
docker build -t registry.adimian.com/ammonite/ammonite .
docker push registry.adimian.com/ammonite/ammonite