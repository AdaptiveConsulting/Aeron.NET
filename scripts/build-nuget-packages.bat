@echo off
pushd %~dp0..
call .\scripts\build.bat
call dotnet pack .\src\Adaptive.Aeron\Adaptive.Aeron.csproj -c Release -o ..\..\nupkgs
call dotnet pack .\src\Adaptive.Agrona\Adaptive.Agrona.csproj -c Release -o ..\..\nupkgs 
call dotnet pack .\src\Adaptive.Cluster\Adaptive.Cluster.csproj -c Release -o ..\..\nupkgs
call dotnet pack .\src\Adaptive.Archiver\Adaptive.Archiver.csproj -c Release -o ..\..\nupkgs
call .\scripts\nuget pack .\driver\Aeron.Driver.nuspec ..\nupkgs
popd