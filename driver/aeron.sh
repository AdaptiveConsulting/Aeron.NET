#!/bin/bash

# ---------------------------------------------------------------------
# startup script to run Aeron Tools.
# ---------------------------------------------------------------------

message()
{
  TITLE="Cannot start" "$1"
  if [ -n "$(command -v zenity)" ]; then
    zenity --error --title="$TITLE" --text="$1" --no-wrap
  elif [ -n "$(command -v kdialog)" ]; then
    kdialog --error "$1" --title "$TITLE"
  elif [ -n "$(command -v notify-send)" ]; then
    notify-send "ERROR: $TITLE" "$1"
  elif [ -n "$(command -v xmessage)" ]; then
    xmessage -center "ERROR: $TITLE: $1"
  else
    printf "ERROR: %s\n%s\n" "$TITLE" "$1"
  fi
}

if [ -z "$(command -v uname)" ] || [ -z "$(command -v realpath)" ] || [ -z "$(command -v dirname)" ] || [ -z "$(command -v cat)" ] || \
   [ -z "$(command -v grep)" ]; then
  TOOLS_MSG="Required tools are missing:"
  for tool in uname realpath grep dirname cat ; do
     test -z "$(command -v $tool)" && TOOLS_MSG="$TOOLS_MSG $tool"
  done
  message "$TOOLS_MSG (SHELL=$SHELL PATH=$PATH)"
  exit 1
fi

# shellcheck disable=SC2034
GREP_OPTIONS=''
OS_TYPE=$(uname -s)
OS_ARCH=$(uname -m)

# ---------------------------------------------------------------------
# Ensure $IDE_HOME points to the directory where the IDE is installed.
# ---------------------------------------------------------------------
AERON_HOME=$(dirname "$(realpath "$0")")
DRIVER_HOME=$(dirname "${AERON_HOME}/driver")
CONFIG_HOME="${XDG_CONFIG_HOME:-${HOME}/.config}"

# ---------------------------------------------------------------------
# Locate a JRE installation directory command -v will be used to run the IDE.
# Try (in order): $JDK_HOME, $JAVA_HOME, "java" in $PATH.
# ---------------------------------------------------------------------
JRE=""

# shellcheck disable=SC2153
if [ -z "$JRE" ]; then
  if [ -n "$JDK_HOME" ] && [ -x "$JDK_HOME/bin/java" ]; then
    JRE="$JDK_HOME"
  elif [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JRE="$JAVA_HOME"
  fi
fi

if [ -z "$JRE" ]; then
  JAVA_BIN=$(command -v java)
else
  JAVA_BIN="$JRE/bin/java"
fi

if [ -z "$JAVA_BIN" ] || [ ! -x "$JAVA_BIN" ]; then
  message "No JRE found. Please make sure \$JDK_HOME, or \$JAVA_HOME point to valid JRE installation."
  exit 1
fi

CLASS_PATH="$DRIVER_HOME/media-driver.jar"

# ---------------------------------------------------------------------
# Run the Aeron program.
# ---------------------------------------------------------------------
IFS="$(printf '\n\t')"

if [ $1 = "AeronStat" ]; then 
    # shellcheck disable=SC2086
    exec "$JAVA_BIN" \
        -classpath "$CLASS_PATH" -Daeron.dir=/dev/shm/aeron-"$USER" io.aeron.samples.AeronStat
elif [ $1 = "AeronStatEmbeddedMediaDriver" ]; then 
    # run this if you do not have MediaDriver running
    # shellcheck disable=SC2086
    exec "$JAVA_BIN" \
        -classpath "$CLASS_PATH" -Daeron.sample.embeddedMediaDriver=true io.aeron.samples.AeronStat
elif [ $1 = "ErrorStat" ]; then 
    # shellcheck disable=SC2086
    exec "$JAVA_BIN" \
        -classpath "$CLASS_PATH" io.aeron.samples.ErrorStat
elif [ $1 = "StreamStat" ]; then 
    # shellcheck disable=SC2086
    exec "$JAVA_BIN" \
        -classpath "$CLASS_PATH" io.aeron.samples.StreamStat   
elif [ $1 = "BacklogStat" ]; then 
    # shellcheck disable=SC2086
    exec "$JAVA_BIN" \
        -classpath "$CLASS_PATH" io.aeron.samples.BacklogStat   
elif [ $1 = "LossStat" ]; then 
    # shellcheck disable=SC2086
    exec "$JAVA_BIN" \
        -classpath "$CLASS_PATH" io.aeron.samples.LossStat   
elif [ $1 = "LogInspector" ]; then 
    # shellcheck disable=SC2086
    exec "$JAVA_BIN" \
        -classpath "$CLASS_PATH" io.aeron.samples.LogInspector $2                            
elif [ $1 = "MediaDriver" ]; then 
    echo Media Driver Started...
    # shellcheck disable=SC2086
    exec "$JAVA_BIN" \
        -classpath "$CLASS_PATH" --illegal-access=warn io.aeron.driver.MediaDriver        
elif [ $1 = "MediaDriverCluster" ]; then 
    echo Media Driver Cluster Started...
    # shellcheck disable=SC2086
    exec "$JAVA_BIN" \
        -classpath "$CLASS_PATH" \
        -Daeron.cluster.ingress.channel=aeron:udp?endpoint=localhost:9010 \
        -Daeron.archive.control.channel=aeron:udp?endpoint=localhost:8010 \
        -Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0 \
        -Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:9011 \
        -Daeron.cluster.members="0,localhost:20000,localhost:20001,localhost:20002,localhost:0,localhost:8010" \
        io.aeron.cluster.ClusteredMediaDriver  
else
    message "Valid parameters are: AeronStat, AeronStatEmbeddedMediaDriver, ErrorStat, StreamStat, BacklogStat, LossStat, LogInspector, MediaDriver or ClusteredMediaDriver"        
fi