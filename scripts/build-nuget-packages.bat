@echo off
pushd %~dp0..
call dotnet pack src\Adaptive.Aeron\Adaptive.Aeron.csproj                -c Release --output ..\..\nupkgs
call dotnet pack src\Adaptive.Agrona\Adaptive.Agrona.csproj              -c Release --output ..\..\nupkgs 
call dotnet pack src\Adaptive.Cluster\Adaptive.Cluster.csproj            -c Release --output ..\..\nupkgs
call dotnet pack src\Adaptive.Archiver\Adaptive.Archiver.csproj          -c Release --output ..\..\nupkgs
call .\scripts\nuget pack .\driver\Aeron.Driver.nuspec                   -OutputDirectory nupkgs
call dotnet pack src\Adaptive.Aeron.Driver.Native\Adaptive.Aeron.Driver.Native.csproj          -c Release --output ..\..\nupkgs
popd