#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
aws s3 cp $SCRIPT_DIR/pbench_x86_64 s3://presto-deploy-infra-and-cluster-a9d5d14
