@echo off
pushd %~dp0..
SET nuget_source=https://api.nuget.org/v3/index.json

del nupkgs\*.nupkg

call dotnet pack src\Adaptive.Aeron\Adaptive.Aeron.csproj                -c Release --output ..\..\nupkgs
call dotnet pack src\Adaptive.Agrona\Adaptive.Agrona.csproj              -c Release --output ..\..\nupkgs 
call .\scripts\nuget pack .\driver\Aeron.Driver.nuspec                   -OutputDirectory nupkgs

call dotnet nuget push nupkgs\Agrona.*.nupkg -s %nuget_source%
call dotnet nuget push nupkgs\Aeron.Client.*.nupkg -s %nuget_source%
call dotnet nuget push nupkgs\Aeron.Driver.*.nupkg -s %nuget_source%

popd