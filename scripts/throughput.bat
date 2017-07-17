@echo off
pushd %~dp0..
.\src\Samples\Adaptive.Aeron.Samples.IpcThroughput\bin\Release\Adaptive.Aeron.Samples.IpcThroughput.exe ^
	-Daeron.sample.messageLength=32
popd