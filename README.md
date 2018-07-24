# ![](./img/Hadoop_logo.png) Apache Hadoop  


An Apache Hadoop collectd plugin which users can use to send metrics from Hadoop clusters to SignalFx

- [Description](#description)
- [Requirements and Dependencies](#requirements-and-dependencies)
- [Installation](#installation)
- [Configuration](#configuration)
- [License](#license)

### DESCRIPTION

This is the SignalFx Apache Hadoop plugin.  Follow [these](https://github.com/signalfx/integrations/blob/master/collectd-hadoop/) instructions to install the Apache Hadoop plugin for collectd & JMX.


This plugin used a python-based collectd integration and JMX configuration. Collectd python for pulling detailed application, queue, node, and MapReduce metrics from resource manager REST API. JMX for pulling namenode, datanode, node manager & high level resource manager metrics

#### Features

##### Built-in Dashboards
- **Hadoop YARN** Resource Manager and Application metrics

  [<img src='./img/yarn_resource_manager.png' width=200px>](./img/yarn_resource_manager.png)
  [<img src='./img/yarn_application.png' width=200px>](./img/yarn_application.png)

- **Hadoop HDFS** HDFS Overview, NameNode, and DataNode metrics

  [<img src='./img/hdfs_overview.png' width=200px>](./img/hdfs_overview.png)
  [<img src='./img/hdfs_namenode.png' width=200px>](./img/hdfs_namenode.png)
  [<img src='./img/hdfs_datanode.png' width=200px>](./img/hdfs_datanode.png)

- **Hadoop MapReduce** MapReduce applications, jobs, and resource usage

  [<img src='./img/mapreduce_apps.png' width=200px>](./img/mapreduce_apps.png)
  [<img src='./img/mapreduce_jobs.png' width=200px>](./img/mapreduce_jobs.png)
  [<img src='./img/mapreduce_usage.png' width=200px>](./img/mapreduce_usage.png)




### REQUIREMENTS AND DEPENDENCIES

>To enable JMX in Hadoop, add the following JVM options to hadoop-env.sh and yarn-env.sh respectively

**hadoop-env.sh:**

    export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=5677 $HADOOP_NAMENODE_OPTS"
    export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=5679 $HADOOP_DATANODE_OPTS"

**yarn-env.sh:**

    export YARN_NODEMANAGER_OPTS="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=5678 $YARN_NODEMANAGER_OPTS"
    export YARN_NODEMANAGER_OPTS="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=5678 $YARN_NODEMANAGER_OPTS"

This plugin requires:

| Software          | Version        |
|-------------------|----------------|
| collectd          |     4.9+       |
| Python plugin for collectd | (included with SignalFx collectd) |
| Python            |     2.6+       |
| Apache Hadoop     |     2.0+       |

### INSTALLATION

For installation, please refer to https://github.com/signalfx/integrations/tree/master/collectd-hadoop.

### CONFIGURATION

>See the following links for more information of specific metric endpoints:

>https://hadoop.apache.org/docs/r2.7.4/hadoop-project-dist/hadoop-common/Metrics.html

>https://hadoop.apache.org/docs/r2.7.4/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html

>https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapredAppMasterRest.html

Using the example configuration file [10-hadoop.conf](https://github.com/signalfx/integrations/blob/master/collectd-hadoop/10-hadoop.conf) as a guide, provide values for the configuration options listed below that make sense for your environment.

| Configuration Option | Definition | Example Value |
| ---------------------|------------|---------------|
| ModulePath* | Path on disk where collectd can find this module. | "/usr/share/collectd/collectd-hadoop" |
| ResourceManagerURL*  | Host where Resource Manager REST API is running | "http://127.0.0.1" |
| ResourceManagerPort*  | Port where Resource Manager REST API is running | 8088 |
| ExcludeMetrics  | An individual metric name to be excluded | "hadoop.cluster.metrics.allocated_mb" |
| Interval | How often this plugin will emit metrics, in second | 10 |
| Dimension | A custom dimension to add to the metrics | Key Value |
| Verbose | Enable verbose logging | true |

\* denotes required configuration option.

### LICENSE

> Include licensing information for this integration metadata, not the integration itself, in this section.

This integration is released under the Apache 2.0 license. See [LICENSE](https://github.com/signalfx/collectd-example/blob/master/LICENSE) for more details.
