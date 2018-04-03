
rem Variables
set SBE_JAR=sbe\sbe-all-1.7.10-SNAPSHOT.jar
set SCHEMA=..\src\Adaptive.Archiver\aeron-archive-codecs.xml
set TEMP_DIR=tmp
set NAMESPACE_DIR=Adaptive\Archiver\Codecs
set OUTPUT_DIR=..\src\Adaptive.Archiver\Codecs

rem Delete output directory
if not exist %OUTPUT_DIR% goto NOOUTPUT
rmdir %OUTPUT_DIR% /s /q

:NOOUTPUT
rem Generate CS files
java -Dsbe.output.dir=%TEMP_DIR% -Dsbe.generate.ir="false" -Dsbe.target.language="csharp" -jar %SBE_JAR% %SCHEMA%

rem Move CS files
move .\%TEMP_DIR%\%NAMESPACE_DIR% %OUTPUT_DIR%

rem Delete temporary files
rmdir %TEMP_DIR% /s /q
