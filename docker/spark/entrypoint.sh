#!/bin/bash

cp -f "$SPARK_HOME"/conf/spark-defaults.conf.template "$SPARK_HOME"/conf/spark-defaults.conf
cp -f "$LIVY_HOME"/conf/livy.conf.template "$LIVY_HOME"/conf/livy.conf
env | grep ^CFG_SPARK_ | sort | python /scripts/env2conf.py >> $SPARK_HOME/conf/spark-defaults.conf
echo "livy.spark.master spark://$(hostname):7077" >> $LIVY_HOME/conf/livy.conf
env | grep ^CFG_LIVY_ | sort | python /scripts/env2conf.py >> $LIVY_HOME/conf/livy.conf

shopt -s extglob
ln -sf /extralib/aws-* $SPARK_HOME/jars/
ln -sf /extralib/!(livy-*) /usr/livy/repl_2.12-jars/
#ln -sf /extralib/* $SPARK_HOME/jars/

$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-history-server.sh
# shellcheck disable=SC2046
$SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077
$SPARK_HOME/sbin/start-thriftserver.sh --master spark://$(hostname):7077
$LIVY_HOME/bin/livy-server start

/scripts/parallel_commands.sh "/scripts/watchdir /usr/spark/logs" "/scripts/watchdir /usr/livy/logs"

$LIVY_HOME/bin/livy-server stop
$SPARK_HOME/sbin/stop-thriftserver.sh
$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-history-server.sh
$SPARK_HOME/sbin/stop-master.sh