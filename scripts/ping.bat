@echo off
call ..\src\Samples\Adaptive.Aeron.Samples.Ping\bin\x64\Release\Adaptive.Aeron.Samples.Ping.exe ^
	-Daeron.sample.messages=1000000 ^
	-Daeron.sample.messageLength=32