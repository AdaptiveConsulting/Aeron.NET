@echo off
pushd %~dp0..
.\src\Samples\Adaptive.Aeron.Samples.IpcThroughput\bin\Release\net45\Adaptive.Aeron.Samples.IpcThroughput.exe ^
	-Daeron.sample.messageLength=32
popd