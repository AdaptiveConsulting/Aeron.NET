@echo off
pushd %~dp0..
call dotnet build /p:Configuration=Release /p:Platform="Any CPU" .\src\Adaptive.Aeron.sln
popd