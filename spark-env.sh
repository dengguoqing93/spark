export LD_LIBRARY_PATH=$JAVA_LIBRARY_PATH
export JAVA_HOME=/opt/software/jdk8
export HADOOP_HOME=/opt/software/hadoop
export HADOOP_CONF_DIR=/opt/software/hadoop/etc/hadoop
export SPARK_DIST_CLASSPATH=$(/oopt/software/hadoop/bin/hadoop classpath)
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g

# 注释以下两行！！！
# export SPARK_MASTER_HOST=master
# export SPARK_MASTER_PORT=7077
# 添加以下内容
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=master,slave0,slave2 -Dspark.deploy.zookeeper.dir=/spark"

# spark-env.sh
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080
-Dspark.history.retainedApplications=50 -Dspark.history.fs.logDirectory=hdfs://linux121:9000/sparkeventlog"