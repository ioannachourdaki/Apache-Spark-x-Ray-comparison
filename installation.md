
# Installation Instructions
----------------------------------------------------------------

If you need to create new keys to use for your cluster, we suggest the below steps:
```bash
$ ssh-keygen -t rsa #press [ENTER] as many times as promted
$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```
Steps to do on worker VMs:
```bash
$ cat ~/.ssh/id_rsa.pub #master
$ vim ~/.ssh/authorized_keys #worker: copy above output and paste it here
```

In the case that there are issues with the permissions of the private key, use `sudo chmod 600 id_rsa`

## Apache Spark

here we will present the steps needed to install Apache Spark along with Hadoop Distributed File System (HDFS) and Yet Another Resource Negotiator (YARN) for handling the big amount of data that is needed for the scripts.

```bash
sudo apt update
```

### Step 1 - Download of Java

For the installation of Java you need the first line of code and you can use the second to verify that the installation was successful.
```bash
$ sudo apt install default-jdk -y
$ java -version
```

### Step 2 - Setup Hadoop and Apache Spark

We will need everything to be in one directory, therefore we suggest doing the following steps:

Note: if you plan on working with other users for this project, we suggest creating a new directory under /home and giving proper permissions to all users involved

```bash
$ mkdir ./opt
$ mkdir ./opt/bin

$ wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
$ tar -xvzf hadoop-3.3.6.tar.gz
$ mv hadoop-3.3.6 ./opt/bin
$ wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
$ tar -xvzf spark-3.5.1-bin-hadoop3.tgz
$ mv ./spark-3.5.1-bin-hadoop3 ./opt/bin/
$ cd ./opt
$ ln -s ./bin/hadoop-3.3.6/ ./hadoop
$ ln -s ./bin/spark-3.5.1-bin-hadoop3/ ./spark
$ cd
$ rm hadoop-3.3.6.tar.gz
$ rm spark-3.5.1-bin-hadoop3.tgz
$ mkdir ~/opt/data
$ mkdir ~/opt/data/hadoop
$ mkdir ~/opt/data/hdfs
```

### Step 3 - Properly setup environment variables

We need to change the file `sudo vim ~/.bashrc` for one user or the `sudo vim /etc/bash.bashrc` for multiple uses and add the following lines to the files:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  #Value should match: dirname $(dirname $(readlink -f $(which java)))
export HADOOP_HOME=/home/user/opt/hadoop
export SPARK_HOME=/home/user/opt/spark
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$SPARK_HOME/bin;
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export LD_LIBRARY_PATH=/home/ubuntu/opt/hadoop/lib/native:$LD_LIBRARY_PATH
export PYSPARK_PYTHON=python3
export CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob`
```

And finally we update the changes we did with `sudo source ~/.bashrc` or `sudo source /etc/bash.bashrc`

### Step 4 - Setup of HDFS

You can find the configuration files of Hadoop in `/home/{directory_name}/opt/hadoop/etc/hadoop`. You can use the environment variable `$HADOOP_HOME` and access it through `$HADOOP_HOME/etc/hadoop/`. Then we have to update 4 files:

We start with `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` and add the following line:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

Then we move on to `$HADOOP_HOME/etc/hadoop/core-site.xml` and change the contents to the ones below:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/home/{directory_name}/opt/data/hadoop</value>
        <description>Parent directory for other temporary directories.</description>
    </property>
    <property>
        <name>fs.defaultFS </name>
        <value>hdfs://master:54310</value>
        <description>The name of the default file system. </description>
    </property>
</configuration>
```

Then we adapt the file `$HADOOP_HOME/etc/hadoop/hdfs-site.xml` as below:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
        <description>Default block replication.</description>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/home/{directory_name}/opt/data/hdfs</value>
    </property>
</configuration>
```

And finally we add the workers to `$HADOOP_HOME/etc/hadoop/workers`:
```
master
worker-1
worker-2
```

### Step 5 - Format and checking that HDFS is operating properly

You need to run the following commands in order for HDFS to format and start:
```bash
 $ $HADOOP_HOME/bin/hdfs namenode -format
 $ start-dfs.sh
```

And then you can find it at `http://xxx.xxx.xxx.xxx:9870` where xxx.xxx.xxx.xxx is your master's public IP. You should be able to see 3 live nodes.

Note: If something doesn't seem to be working properly, try to fix it but we suggest you to not run the format command line more than once. It will force you to uninstall and reinstall everything from the beginnning.

### Step 6 - YARN setup

First you need to update the `$HADOOP_HOME/etc/hadoop/yarn-site.xml` file as follows:
```xml
<?xml version="1.0"?>
<configuration>
<!-- Site specific YARN configuration properties -->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>master</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <!--Insert the public IP of your master machine here-->
        <value>xxx.xxx.xxx.xxx:8088</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>6144</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>6144</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>128</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
   <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle,spark_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
        <value>org.apache.spark.network.yarn.YarnShuffleService</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.spark_shuffle.classpath</name>
        <value>/home/{directory_name}/opt/spark/yarn/*</value>
    </property>
</configuration>
```

And to start YARN you can run `$ start-yarn.sh`. To make sure it initiated successfully you can check the URL `http://xxx.xxx.xxx.xxx::8088/cluster` and you should be able to see 3 live nodes.

### Step 7 - Spark History Server setup (optional)

We create `$SPARK_HOME/conf/spark-defaults.conf` and then add the bellow information:
```
spark.eventLog.enabled          true
spark.eventLog.dir              hdfs://master:54310/spark.eventLog
spark.history.fs.logDirectory   hdfs://master:54310/spark.eventLog
spark.master                    yarn
spark.submit.deployMode         client
spark.driver.memory             1g
spark.executor.memory           1g
spark.executor.cores            1
```

Finally we create the directory for the event logs and we start spark history server:
```bash
$ hadoop fs -mkdir /spark.eventLog
$ $SPARK_HOME/sbin/start-history-server.sh
```

And you can check it once more through `http://xxx.xxx.xxx.xxx::18080`

### Step 8 - Firewall setup (optional)

During our trials we realised that YARN API is susceptible to hacking and therefore after some time of testing we were unable to use any of our machines and they had been compromised. Therefore we propose for the students and professors of NTUA the following steps in order to create a secure firewall.

First, you need to be certain that you can login in the [openVPN GUI for NTUA](https://www.noc.ntua.gr/help/VPN). Then you need to create a firewall rule that:
* Type: Ingress
* Targets: Apply to all
* IP Ranges: 147.102.0.0/16
* Protocols: tcp:8088, 9870, 18080, udp:8088, 9870, 18080
* Action: Allow
* Priority: high

And if you do that, you shouldn't have any issue with hackers and should be able to run your tests safely.

For apache Spark initiation and stop we provide you with 2 bash scripts that start and stop respectively HDFS and YARN, named `startSpark.sh` and `stopSpark.sh`.

## Ray

Ray is simpler to install in comparison to Apache Spark. First of all, it needs to be mentioned that while it can use HDFS, we encountered many difficulties therefore if you can use the files locally, we suggest you do so. 

The installation is as follows for all machines:
```bash
sudo pip install ray
```

And then other requirements are needed which you can find in the `Requirements.txt` file.

Then, in order to initiate Ray you need one command in master and one in each of the workers as follows:
```bash
ray start --address=[head-node-address] # Master
ray start --head --node-ip-address=[head-node-address] --port=6379 # Workers
```