# GraphX-Pagerank
## Overview

Use GraphX Platform to Implement Pagerank Algorithm

## Getting Started

### Hadoop

Go to your favorite directory and download Hadoop soure file

```shell
cd $YOUR_FAVORITE_PATH
wget http://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
tar xzvf hadoop-3.2.1.tar.gz
```

Configure environment variables in `~/.bashrc` (if you use `bash` )

```shell
# Hadoop Variables Start
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP=$YOUR_FAVORITE_PATH/hadoop-3.2.1
export PATH=$PATH:$HADOOP/bin:$HADOOP/sbin
export HADOOP_MAPRED_HOME=$HADOOP
export HADOOP_COMMON_HOME=$HADOOP
export HADOOP_HDFS_HOME=$HADOOP
export YARN_HOME=$HADOOP_INSTALL
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP/lib"
# Hadoop Variables End
```

 Your `JAVA_HOME` and `HADOOP` should be adjusted according to your system. 

Change `JAVA_HOME` in `hadoop-env.sh` file

```shell
echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> $YOUR_FAVORITE_PATH/hadoop-3.2.1/etc/hadoop/hadoop-env.sh
```

Use `WordCount` program to test the configuration of `hadoop`. 

```shell
hadoop jar share/hadoop/mapreduce/sources/hadoop-mapreduce-examples-3.2.1-sources.jar org.apache.hadoop.examples.WordCount input output
```

