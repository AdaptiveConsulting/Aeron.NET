@echo off
echo Media Driver Started...
java -cp media-driver.jar -Daeron.cluster.ingress.channel=aeron:udp?endpoint=localhost:9010 -Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:9011 ^
	io.aeron.cluster.ClusteredMediaDriver
echo Media Driver Stopped.
pause