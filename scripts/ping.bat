@echo off
pushd %~dp0..
call .\src\Samples\Adaptive.Aeron.Samples.Ping\bin\Release\net45\Adaptive.Aeron.Samples.Ping.exe ^
	-Daeron.sample.messages=1000000 ^
	-Daeron.sample.messageLength=32
popd