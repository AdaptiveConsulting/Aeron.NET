@echo off
echo Media Driver Started...
java -cp media-driver.jar ^
	io.aeron.cluster.ClusteredMediaDriver
echo Media Driver Stopped.
pause