@echo off
pushd %~dp0..
.\src\Samples\Adaptive.Aeron.Samples.StreamingPublisher\bin\Release\net45\Adaptive.Aeron.Samples.StreamingPublisher.exe ^
	-Daeron.sample.messageLength=32 ^
	-Daeron.sample.messages=500000000
popd