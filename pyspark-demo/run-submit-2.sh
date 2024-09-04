#!/bin/bash

export SPARK_HOME="/home/schemer/.opt/spark247"
export JAVA_HOME="/usr/lib/jvm/default"
export DATA_SOURCE="/data/super_spark_data"
export DATA_DESTINATION="/data/super_spark_data"

export APP_NAME="pyspark-demo"

MAIN_FILE=$1
shift
MAIN_ARGS=$@

DEFAULT_MASTER_HOST='local[*]'
MASTER_HOST=${MASTER_HOST:-$DEFAULT_MASTER_HOST}

DEFAULT_INTERPRETOR=${PYSPARK_PYTHON:-'/home/schemer/.local/share/virtualenvs/pyspark-demo-zkuKtgL5/bin/python'}
INTERPRETOR=${INTERPRETOR:-$DEFAULT_INTERPRETOR}

DEFAULT_HOST_OF_HOST='localhost'
HOST_OF_HOST=${HOST_OF_HOST:-$DEFAULT_HOST_OF_HOST}

# BASIC_OPTS=''
BASIC_OPTS='-XX:+UnlockExperimentalVMOptions -XX:+UnlockDiagnosticVMOptions -XX:+AlwaysPreTouch -XX:+UseNUMA'

JMX_OPTS=''
#JMX_OPTS="
#-Dcom.sun.management.jmxremote=true \
#-Dcom.sun.management.jmxremote.local.only=false \
#-Dcom.sun.management.jmxremote.port=9010 \
#-Dcom.sun.management.jmxremote.authenticate=false \
#-Dcom.sun.management.jmxremote.ssl=false \
#-Dcom.sun.management.jmxremote.rmi.port=9010 \
#-Djava.rmi.server.hostname=$HOST_OF_HOST"

# GC_OPTS=''
# GC_OPTS='-XX:+UseEpsilonGC '
# GC_OPTS='-XX:+UseShenandoahGC -XX:-UseBiasedLocking -XX:+DisableExplicitGC'
GC_OPTS='-XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:MaxGCPauseMillis=50  -XX:ParallelGCThreads=12 -XX:ConcGCThreads=12'
# GC_OPTS='-XX:+UseZGC -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+ZProactive -XX:MaxGCPauseMillis=50 -XX:ParallelGCThreads=12 -XX:ConcGCThreads=12'
# GC_OPTS='-XX:+UseZGC -XX:+ZProactive -XX:MaxGCPauseMillis=50 -XX:ParallelGCThreads=12 -XX:ConcGCThreads=12'

# MODULE_OPTS='--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

DEFAULT_JVM_OPT="$BASIC_OPTS $GC_OPTS $JMX_OPTS"
JVM_OPTION=${JVM_OPTION:-$DEFAULT_JVM_OPT}

DEFAULT_DRIVER_CORES='4'
DRIVER_CORES=${DRIVER_CORES:-$DEFAULT_DRIVER_CORES}

DEFAULT_DRIVER_MEMORY='8g'
DRIVER_MEMORY=${DRIVER_MEMORY:-$DEFAULT_DRIVER_MEMORY}

DEFAULT_EXECUTOR_CORES='16'
EXECUTOR_CORES=${EXECUTOR_CORES:-$DEFAULT_EXECUTOR_CORES}

DEFAULT_EXECUTOR_MEMORY='54g'
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-$DEFAULT_EXECUTOR_MEMORY}

DEFAULT_NETWORK_TIMEOUT='10000000'
NETWORK_TIMEOUT=${NETWORK_TIMEOUT:-$DEFAULT_NETWORK_TIMEOUT}

DEFAULT_HEARTBEAT_INTERVAL='10000000'
HEARTBEAT_INTERVAL=${HEARTBEAT_INTERVAL:-$DEFAULT_HEARTBEAT_INTERVAL}

DEFAULT_MEMORY_FRACTION='0.45'
MEMORY_FRACTION=${MEMORY_FRACTION:-$DEFAULT_MEMORY_FRACTION}

DEFAULT_STORAGE_FRACTION='0.75'
STORAGE_FRACTION=${STORAGE_FRACTION:-$DEFAULT_STORAGE_FRACTION}

rm -rf /data/super_spark_data/stat_transactions/*

$SPARK_HOME/bin/spark-submit \
    --master="$MASTER_HOST" \
    --conf "spark.pyspark.python=$INTERPRETOR" \
    --conf "spark.pyspark.driver.python=$INTERPRETOR" \
    --conf "spark.driver.cores=$DRIVER_CORES" \
    --conf "spark.driver.memory=$DRIVER_MEMORY" \
    --conf "spark.driver.maxResultSize=0" \
    --conf "spark.driver.extraJavaOptions=$JVM_OPTION" \
    --conf "spark.executor.cores=$EXECUTOR_CORES" \
    --conf "spark.executor.memory=$EXECUTOR_MEMORY" \
    --conf "spark.executor.extraJavaOptions=$JVM_OPTION" \
    --conf "spark.network.timeout=$NETWORK_TIMEOUT" \
    --conf "spark.executor.heartbeatInterval=$HEARTBEAT_INTERVAL" \
    ./$MAIN_FILE $MAIN_ARGS
#     --conf "spark.memory.fraction=$MEMORY_FRACTION" \
#     --conf "spark.storage.memoryFraction=$STORAGE_FRACTION" \
