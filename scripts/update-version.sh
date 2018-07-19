#!/bin/sh
BASEDIR=$(readlink -f $(dirname "$0"))
cd $BASEDIR/..

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <FROM_VERSION> <TO_VERSION>"
    exit -1
fi

FROM_VERSION=$1
TO_VERSION=$2

sed -i s/$FROM_VERSION/$TO_VERSION/ driver/Aeron.Driver.nuspec src/Adaptive.Aeron/Adaptive.Aeron.csproj src/Adaptive.Agrona/Adaptive.Agrona.csproj src/Adaptive.Archiver/Adaptive.Archiver.csproj src/Adaptive.Cluster/Adaptive.Cluster.csproj
