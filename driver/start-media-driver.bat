@echo off
echo Starting Media Driver...
java -cp media-driver.jar ^
	io.aeron.driver.MediaDriver -Daeron.threading.mode=SHARED
echo Media Driver Stopped.
pause