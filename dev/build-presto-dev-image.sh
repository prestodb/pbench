#!/bin/sh

set -e

if [ ! $# -eq 1 ]; then
    echo "$0 [path to presto repo]"
    exit 1
elif [ ! -d $1 ]; then
    echo "$1 is not a valid path"
    exit 1
fi

DOCKERFILE=$(dirname $(realpath $0))/Dockerfile-presto-dev
echo $DOCKERFILE
cd $1
PRESTO_VERSION=$(./mvnw org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)
HEAD_SHA=$(git rev-parse HEAD)
HEAD_SHA=${HEAD_SHA:0:7}
echo "Presto version is $PRESTO_VERSION, head SHA is $HEAD_SHA"
# docker buildx build --load --platform "linux/amd64" -t "presto-dev-$HEAD_SHA" \
#     --build-arg "PRESTO_VERSION=${PRESTO_VERSION}" \
#     -f $DOCKERFILE .
docker buildx build -t "presto-dev-$HEAD_SHA" \
    --build-arg "PRESTO_VERSION=${PRESTO_VERSION}" \
    -f $DOCKERFILE .
