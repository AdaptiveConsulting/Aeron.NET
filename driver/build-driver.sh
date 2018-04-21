#!/bin/sh
set -e

WD=`pwd`
VERSION_FILE=$WD/version.txt
TMP_DIR=$WD/build

AGRONA_BUILD_DIR=$TMP_DIR/agrona

SBE_BUILD_DIR=$TMP_DIR/sbe

AERON_BUILD_DIR=$TMP_DIR/aeron
AERON_PATCH=$WD/aeron.patch

[ -d $TMP_DIR ] || mkdir -p $TMP_DIR

[ -d $AGRONA_BUILD_DIR ] || git clone -q https://github.com/real-logic/agrona.git $AGRONA_BUILD_DIR
[ -d $SBE_BUILD_DIR ] || git clone -q https://github.com/real-logic/simple-binary-encoding.git $SBE_BUILD_DIR
[ -d $AERON_BUILD_DIR ] || git clone -q https://github.com/real-logic/aeron.git $AERON_BUILD_DIR

cd $AGRONA_BUILD_DIR
git fetch -q
[ -z "$AGRONA_VERSION" ] && AGRONA_VERSION=`git describe --tags origin/master`
echo Building Agrona $AGRONA_VERSION
git checkout -qf $AGRONA_VERSION
./gradlew -x test
cd $WD

cd $SBE_BUILD_DIR
git fetch -q
[ -z "$SBE_VERSION" ] && SBE_VERSION=`git describe --tags origin/master`
echo Building SBE $SBE_VERSION
git checkout -qf $SBE_VERSION
./gradlew -x test
cd $WD


cd $AERON_BUILD_DIR
git fetch -q
[ -z "$AERON_VERSION" ] && AERON_VERSION=`git describe --tags origin/master`
echo Building Aeron $AERON_VERSION
git checkout -qf $AERON_VERSION
./gradlew -x test
cd $WD

echo "Driver built from source" > $VERSION_FILE
echo "Agrona: $AGRONA_VERSION" >> $VERSION_FILE
echo "SBE:    $SBE_VERSION" >> $VERSION_FILE
echo "Aeron:  $AERON_VERSION" >> $VERSION_FILE

cp $AERON_BUILD_DIR/aeron-all/build/libs/aeron-all-*-SNAPSHOT.jar $WD/media-driver.jar
