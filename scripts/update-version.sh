#!/bin/sh
BASEDIR=$(cd "$(dirname "$0")" && pwd)
cd "$BASEDIR/.."

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <FROM_VERSION> <TO_VERSION>"
    exit 1
fi

FROM_VERSION=$1
TO_VERSION=$2

FROM_MINOR=$(echo "$FROM_VERSION" | cut -d. -f2)
TO_MINOR=$(echo "$TO_VERSION" | cut -d. -f2)

# GNU sed uses `-i ''` differently than BSD sed; use a portable form via -i.bak then remove backups.
sed -i.bak "s/$FROM_VERSION/$TO_VERSION/g" \
    driver/Aeron.Driver.nuspec \
    src/Adaptive.Aeron/Adaptive.Aeron.csproj \
    src/Adaptive.Agrona/Adaptive.Agrona.csproj \
    src/Adaptive.Archiver/Adaptive.Archiver.csproj \
    src/Adaptive.Cluster/Adaptive.Cluster.csproj \
    src/Adaptive.Aeron/AeronVersion.cs

sed -i.bak "s/MINOR_VERSION = $FROM_MINOR/MINOR_VERSION = $TO_MINOR/g" \
    src/Adaptive.Aeron/AeronVersion.cs

find driver src -name '*.bak' -delete
