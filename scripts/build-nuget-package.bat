@echo off
pushd %~dp0..
call .\scripts\build.bat
call .\scripts\nuget pack .\src\Adaptive.Aeron\Adaptive.Aeron.csproj -IncludeReferencedProjects -Prop Configuration=Release
popd