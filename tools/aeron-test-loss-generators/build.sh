#!/usr/bin/env bash
# Builds aeron-test-loss-generators.jar from sources in src/main/java, using the
# bundled aeron-all jar in ../../driver/media-driver.jar as the compile classpath.
# Output: target/aeron-test-loss-generators.jar and a copy at ../../driver/.
#
# Re-run after editing any source file. The driver/ copy is what EmbeddedMediaDriver
# loads at test time.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
src="$here/src/main/java"
out="$here/target/classes"
jar_out="$here/target/aeron-test-loss-generators.jar"
deployed="$here/../../driver/aeron-test-loss-generators.jar"
driver_jar="$here/../../driver/media-driver.jar"

if [[ ! -f "$driver_jar" ]]; then
    echo "ERROR: $driver_jar not found — driver/media-driver.jar must exist" >&2
    exit 1
fi

rm -rf "$out"
mkdir -p "$out"

find "$src" -name '*.java' -print0 | xargs -0 javac \
    -source 17 -target 17 -encoding UTF-8 \
    -classpath "$driver_jar" \
    -d "$out"

(cd "$out" && jar -cf "$jar_out" .)
cp "$jar_out" "$deployed"

echo "Built: $jar_out"
echo "Deployed: $deployed"
