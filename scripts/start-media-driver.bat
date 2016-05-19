@echo off
echo Starting...
java -cp media-driver-0.9.7.jar ^
	io.aeron.driver.MediaDriver ipc.properties 