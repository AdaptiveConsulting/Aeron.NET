@echo off
echo Media Driver Started...
java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED -cp media-driver.jar io.aeron.driver.MediaDriver
echo Media Driver Stopped.
pause