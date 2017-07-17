@echo off
pushd %~dp0..
.\scripts\nuget restore .\src\Adaptive.Aeron.sln
call "C:\Program Files (x86)\MSBuild\14.0\Bin\MsBuild.exe" /p:Configuration=Release /p:Platform="Any CPU" .\src\Adaptive.Aeron.sln
popd