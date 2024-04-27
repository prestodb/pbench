#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cp -r $SCRIPT_DIR/cluster-configs/* $SCRIPT_DIR/../presto-performance/presto-deploy-cluster/clusters
cp -r $SCRIPT_DIR/genconfig/templates $SCRIPT_DIR/../presto-performance/presto-deploy-cluster/clusters
