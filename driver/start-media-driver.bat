@echo off
echo Starting Media Driver...
java -cp media-driver.jar ^
	io.aeron.driver.MediaDriver ipc.properties 