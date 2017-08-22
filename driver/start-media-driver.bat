@echo off
echo Media Driver Started...
java -cp media-driver.jar ^
	io.aeron.driver.MediaDriver ipc.properties
echo Media Driver Stopped.
pause