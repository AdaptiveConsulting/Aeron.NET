@echo off
pushd %~dp0..
SET suffix=alpha
SET nuget_source=https://www.myget.org/F/aeron/api/v2/package

del nupkgs\*.nupkg

call dotnet pack src\Adaptive.Aeron\Adaptive.Aeron.csproj                -c Release --version-suffix "%suffix%" --output ..\..\nupkgs
call dotnet pack src\Adaptive.Agrona\Adaptive.Agrona.csproj              -c Release --version-suffix "%suffix%" --output ..\..\nupkgs 
call dotnet pack src\Adaptive.Cluster\Adaptive.Cluster.csproj            -c Release --version-suffix "%suffix%" --output ..\..\nupkgs
call dotnet pack src\Adaptive.Archiver\Adaptive.Archiver.csproj          -c Release --version-suffix "%suffix%" --output ..\..\nupkgs
call .\scripts\nuget pack .\driver\Aeron.Driver.nuspec                   -OutputDirectory nupkgs -suffix "%suffix%"

call dotnet nuget push nupkgs\Agrona.*.nupkg -s %nuget_source%
call dotnet nuget push nupkgs\Aeron.Client.*.nupkg -s %nuget_source%
call dotnet nuget push nupkgs\Aeron.Driver.*.nupkg -s %nuget_source%
call dotnet nuget push nupkgs\Aeron.Cluster.*.nupkg -s %nuget_source%
call dotnet nuget push nupkgs\Aeron.Archiver.*.nupkg -s %nuget_source%

popd