@echo off
pushd %~dp0..
call .\scripts\build.bat
call dotnet pack .\src\Adaptive.Aeron\Adaptive.Aeron.csproj /p:Configuration=Release
call dotnet pack .\src\Adaptive.Agrona\Adaptive.Agrona.csproj /p:Configuration=Release
call .\scripts\nuget pack .\driver\Aeron.Driver.nuspec
popd