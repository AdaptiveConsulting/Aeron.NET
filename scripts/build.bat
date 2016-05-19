@echo off
nuget restore ..\src\Adaptive.Aeron.sln
call "C:\Program Files (x86)\MSBuild\14.0\Bin\MsBuild.exe" /p:Configuration=Release /p:Platform="x64" ..\src\Adaptive.Aeron.sln