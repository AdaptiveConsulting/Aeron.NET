@echo off
java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED -cp media-driver.jar io.aeron.cluster.ClusterControl SNAPSHOT
pause