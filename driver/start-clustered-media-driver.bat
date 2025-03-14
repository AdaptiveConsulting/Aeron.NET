@echo off
echo Media Driver Started...
java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED -cp media-driver.jar -Daeron.cluster.ingress.channel=aeron:udp?endpoint=localhost:9010 -Daeron.archive.control.channel=aeron:udp?endpoint=localhost:8010 -Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0 -Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:9011 -Daeron.cluster.members="0,localhost:20000,localhost:20001,localhost:20002,localhost:0,localhost:8010" ^
	io.aeron.cluster.ClusteredMediaDriver
echo Media Driver Stopped.
pause