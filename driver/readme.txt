Aeron Media Driver
==================

This is the Java version of the Aeron Media Driver for use with the Aeron Client.

To Run, open the 'driver' folder in explorer and double click on the BAT file. A command prompt window should open with 'Starting Media Driver...' 

You will need Java Runtime Environment (JRE) Version 8 to run the Media Driver which you can get here: http://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html

This package creates a directory in your solution called 'driver' which contains 'media-driver.jar' and a batch file to launch it with default parameters called 'start-media-driver.bat'.

The media driver is also available as source from https://github.com/real-logic/Aeron.

If the media driver is closed, you need to wait 10 seconds before re-opening it.

Note: The batch file assumes you have the Java 8 Runtime in your PATH.