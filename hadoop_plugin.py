import collectd
import metrics
import urllib2
import json

from urlparse import urljoin

PLUGIN_NAME = "apache_hadoop"
RESOURCE_MANAGER_ENDPOINT = 'ws/v1/cluster'
CLUSTER_METRICS_PATH = 'metrics'
CLUSTER_SCHEDULER_PATH = 'scheduler'
CLUSTER_APPS_PATH = 'apps'
CLUSTER_NODES_PATH = 'nodes'
MAPREDUCE_PATH = 'ws/v1/mapreduce'
MAPREDUCE_JOBS_PATH = 'jobs'
MAPREDUCE_APP_TYPE = 'MAPREDUCE'


def str_to_bool(value):
    """Python 2.x does not have a casting mechanism for booleans.  The built in
    bool() will return true for any string with a length greater than 0.  It
    does not cast a string with the text "true" or "false" to the
    corresponding bool value.  This method is a casting function.  It is
    insensitive to case and leading/trailing spaces.  An Exception is raised
    if a cast can not be made.
    """
    if str(value).strip().lower() == "true":
        return True
    elif str(value).strip().lower() == "false":
        return False
    else:
        raise Exception("Unable to cast value (%s) to boolean" % value)


def _validate_url(url):
    return url.startswith("http://")


def _validate_kv(kv):
    """
    check for malformed data on split
    Args:
    kv (list): List of key value pair
    Returns:
    bool: True if list contained expected pair and False otherwise
    """
    if len(kv) == 2 and '' not in kv:
        return True
    return False


def _add_metrics_to_set(set, metrics):
    metrics_list = metrics.strip().split(",")
    for metric in metrics_list:
        set.add(metric)


class MetricRecord(object):
    """
    Struct for all information needed to emit a single collectd metric.
    MetricSink is the expected consumer of instances of this class.
    """

    TO_STRING_FORMAT = '[name={},type={},value={},dimensions={}]'

    def __init__(self, metric_name, metric_type, value,
                 dimensions={}):
        self.name = metric_name
        self.type = metric_type
        self.value = value
        self.dimensions = dimensions

    def to_string(self):
        return MetricRecord.TO_STRING_FORMAT.format(self.name,
                                                    self.type,
                                                    self.value,
                                                    self.dimensions)


class MetricSink(object):
    """
    Responsible for transforming and dispatching a MetricRecord via collectd.
    """

    def emit(self, metric_record):
        """
        Construct a single collectd Values instance from the given MetricRecord
        and dispatch.
        """
        emit_value = collectd.Values()
        emit_value.plugin = PLUGIN_NAME
        emit_value.values = [metric_record.value]
        emit_value.type = metric_record.type
        emit_value.type_instance = metric_record.name
        if metric_record.dimensions:
            emit_value.plugin_instance = '[{0}]'.format(
                self._format_dimensions(metric_record.dimensions))

        # With some versions of CollectD, a dummy metadata map must to be added
        # to each value for it to be correctly serialized to JSON by the
        # write_http plugin. See
        # https://github.com/collectd/collectd/issues/716
        emit_value.meta = {'true': 'true'}

        emit_value.dispatch()

    def _format_dimensions(self, dimensions):
        """
        Formats a dictionary of key/value pairs
        as a comma-delimited list of key=value tokens.
        Taken from docker-collectd-plugin.
        """
        return ','.join(['='.join((key.replace('.', '_'), value))
                        for key, value in dimensions.iteritems()])


class HadoopCollector():
    def __init__(self, resource_manager_url, resource_manager_port,
                 excluded_metrics, custom_dimensions, verbose=False):
        self.metric_sink = MetricSink()
        self.resource_manager_url = resource_manager_url
        self.resource_manager_port = resource_manager_port
        self.excluded_metrics = excluded_metrics
        self.custom_dimensions = custom_dimensions
        self.verbose = verbose
        collectd.info("hadoop : Successfully configured Hadoop Collector ...")

    def log_verbose(self, msg):
        """
        log messages if the instance has been configured with the Verbose option
        """
        if self.verbose:
            collectd.info("hadoop : {0}".format(msg))

    def get_json_from_rest_request(self, url, path, *args, **kwargs):
        """
        Makes REST call and converts response to JSON
        """

        resp = None
        resp = self.rest_request(url, path, *args, **kwargs)
        if not resp:
            collectd.log_verbose("empty json response from API call {0}/{1}".format(
                                 url, path))
            return {}

        try:
            return json.loads(resp)
        except (ValueError, TypeError) as e:
            collectd.warning("hadoop : issue parsing JSON from API call ({0}) {1}/{2}".format(
                             e, url, path))
            return {}

    def rest_request(self, url, path, *args, **kwargs):
        """
        Makes REST call to Hadoop API endpoint
        """
        url = url.rstrip('/')+"/"+path.lstrip('/')
        if args:
            for arg in args:
                url = url.rstrip('/')+"/"+arg.lstrip('/')

        if kwargs:
            query = '&'.join(['{0}={1}'.format(key, value)
                             for key, value in kwargs.iteritems()])
            url = urljoin(url, '?' + query)

        try:
            req = urllib2.Request(url)
            data = urllib2.urlopen(req)
            resp = data.read()
            return resp
        except (urllib2.HTTPError, urllib2.URLError) as e:
            if not (isinstance(e, urllib2.HTTPError) and e.code == 404):
                collectd.warning("hadoop : Unable to make request at (%s) %s" % (e, url))
            return None
        # TODO: figure out what other specific exceptions should be caught
        # and remove the generic exception
        except Exception:
            return None

    def read_callback(self):
        """
        Makes API calls for cluster metrics
        and posts them to SignalFx
        """
        rm_url = self.resource_manager_url+":"+self.resource_manager_port
        self.get_cluster_metrics(rm_url)
        self.get_scheduler_metrics(rm_url)
        self.get_apps_metrics(rm_url)
        self.get_node_metrics(rm_url)
        self.get_mapreduce_metrics(rm_url)

    def get_cluster_metrics(self, rm_url):
        """
        Collects metrics about the cluster from
        <host>/ws/v1/cluster/metrics
        """
        dim = {}
        dim.update(self.custom_dimensions)

        rm_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_METRICS_PATH)
        if not rm_resp:
            self.log_verbose("no cluster metrics found {0}".format(rm_resp))

        for cluster_metric, cm_value in rm_resp.get('clusterMetrics', {}).iteritems():
            for key, (metric_type, metric_name) in metrics.HADOOP_CLUSTER_METRICS.iteritems():
                if key in cluster_metric and metric_name not in self.excluded_metrics:
                    self.metric_sink.emit(MetricRecord(metric_name, metric_type, cm_value, dim))

    def get_node_metrics(self, rm_url):
        """
        Collects metrics about nodes from
        <host>/ws/v1/cluster/nodes
        """
        nodes_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_NODES_PATH)

        nodes = nodes_resp.get('nodes', {}).get('node', [])
        if not nodes:
            self.log_verbose("no nodes returned {0}".format(nodes_resp))

        for node in nodes:
            dim = {"node_rack": node.get('rack'), "state": node.get('state'), "node_id": node.get('id')}
            dim.update(self.custom_dimensions)
            for key, (metric_type, metric_name) in metrics.HADOOP_NODE_METRICS.iteritems():
                if key in node and metric_name not in self.excluded_metrics:
                    self.metric_sink.emit(MetricRecord(metric_name, metric_type, node.get(key), dim))

    def get_apps_metrics(self, rm_url):
        """
        Collects metrics about apps from
        <host>/ws/v1/cluster/apps
        """
        apps_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_APPS_PATH,
                                                    states="accepted,running")

        apps = apps_resp.get('apps', {}).get('app', [])
        if not apps:
            self.log_verbose("no apps returned {0}".format(apps_resp))

        for app in apps:
            dim = {"app_name": app.get('name'), "queue_name": app.get('queue'), "user": app.get('user'),
                   "applicationType": app.get('applicationType')}
            dim.update(self.custom_dimensions)
            for key, (metric_type, metric_name) in metrics.HADOOP_APPLICATIONS.iteritems():
                if key in app and metric_name not in self.excluded_metrics:
                    self.metric_sink.emit(MetricRecord(metric_name, metric_type, app.get(key), dim))

    def get_mapreduce_metrics(self, rm_url):
        """
        Collects metrics about mapreduce jobs
        """
        running_apps = self.get_running_mapreduce_apps(rm_url)
        jobs_metrics = self.get_jobs_from_apps(running_apps)
        return jobs_metrics

    def get_running_mapreduce_apps(self, rm_url):
        """
        Collects a list of all running mapreduce jobs in the cluster from
        <host>/ws/v1/cluster/apps
        """
        running_apps = {}
        apps_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_APPS_PATH,
                                                    states="running", applicationTypes=MAPREDUCE_APP_TYPE)

        apps = apps_resp.get('apps', {}).get('app', [])
        if not apps:
            self.log_verbose("no mapreduce apps returned {0}".format(apps_resp))

        for app in apps:
            app_id = app.get('id')
            app_name = app.get('name')
            app_url = app.get('trackingUrl')

            if app_id and app_name and app_url:
                running_apps[app_id] = (app_name, app_url)

        return running_apps

    def get_jobs_from_apps(self, running_apps):
        """
        Collects metrics about the cluster from
        <host>/ws/v1/mapreduce/jobs
        """
        for app_id, (app_name, trackingUrl) in running_apps.iteritems():
            jobs_resp = self.get_json_from_rest_request(trackingUrl, MAPREDUCE_PATH, MAPREDUCE_JOBS_PATH)

            jobs = jobs_resp.get('jobs', {}).get('job', [])
            if not jobs:
                self.log_verbose("no jobs returned {0}".format(jobs_resp))

            for job in jobs:
                dim = {"user": job.get('user'), "job_id": job.get('id'), "job_name": job.get('name'),
                       "app_id": app_id, "app_name": app_name}
                dim.update(self.custom_dimensions)

                for key, (metric_type, metric_name) in metrics.MAPREDUCE_JOB_METRICS.iteritems():
                    if key in job and metric_name not in self.excluded_metrics:
                        self.metric_sink.emit(MetricRecord(metric_name, metric_type, job.get(key), dim))

    def get_scheduler_metrics(self, rm_url):
        """
        Collects metrics about the cluster from
        <host>/ws/v1/cluster/scheduler
        """
        def get_queue_metrics(queue):
            # recursively collect queue metrics from all sub-queues
            for sub_queue in queue.get('queues', {}).get('queue', []):
                get_queue_metrics(sub_queue)

            # initialize dimensions with custom dimensions
            dim = {}
            dim.update(self.custom_dimensions)

            queue_metric = {}

            # build the queue metric and dimensions based on queue type
            queue_type = queue.get('type')
            if queue_type == 'capacitySchedulerLeafQueueInfo':
                queue_metric = metrics.HADOOP_LEAF_QUEUE
                dim.update({"queue_name": (queue.get('queueName')), "queue_state": (queue.get('state')),
                            "queue_type": queue_type})
            elif queue_type == 'capacityScheduler':
                queue_metric = metrics.HADOOP_ROOT_QUEUE
                dim.update({"queue_name": (queue.get('queueName')), "queue_type": queue_type})
            elif queue_type == 'fifoScheduler':
                queue_metric = metrics.HADOOP_FIFO_SCHEDULER
            elif queue_type is None:
                queue_metric = metrics.HADOOP_LEAF_QUEUE
                dim.update({"queue_name": (queue.get('queueName')), "queue_state": (queue.get('state')),
                            "queue_type": 'capacitySchedulerLeafQueueInfo'})

            for cluster_metric, cm_value in queue.iteritems():
                for key, (metric_type, metric_name) in queue_metric.iteritems():
                    if key in cluster_metric and metric_name not in self.excluded_metrics:
                        self.metric_sink.emit(MetricRecord(metric_name, metric_type, cm_value, dim))

        scheduler_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_SCHEDULER_PATH)

        queues = scheduler_resp.get('scheduler', {}).get('schedulerInfo', {})
        if not queues:
            self.log_verbose("no queues returned {0}".format(scheduler_resp))

        get_queue_metrics(queues)


def configure_callback(conf):
    """
    A callback method that  loads information from the Hadoop collectd plugin config file.
    Args:
    config_values (collectd.Config): Object containing config values
    """
    collectd.info("hadoop : Configuring hadoop plugins")

    resource_manager_url = None
    resource_manager_port = None
    exclude = set()
    interval = None
    custom_dimensions = {}
    verbose = False

    for node in conf.children:
        if node.key == 'ResourceManagerURL' and node.values:
            if not _validate_url(node.values[0]):
                raise ValueError("URL is not prefixed with http://")
            resource_manager_url = node.values[0]
        elif node.key == 'ResourceManagerPort' and node.values:
            resource_manager_port = str(int(node.values[0]))
        elif node.key == 'Interval' and node.values:
            interval = node.values[0]
        elif node.key == 'Dimension':
            if len(node.values) == 2:
                custom_dimensions.update({node.values[0]: node.values[1]})
            else:
                collectd.warning("hadoop : Check configuration \
                                            setting for %s" % node.key)
        elif node.key == 'ExcludeMetrics' and node.values:
            _add_metrics_to_set(exclude, node.values[0])
        elif node.key == "Verbose" and node.values:
            verbose = str_to_bool(node.values[0])
        else:
            collectd.warning("hadoop : Unknown or Malformed configuration setting {0}".format(node))

    if resource_manager_url is None or resource_manager_port is None:
        collectd.error("hadoop : Resource Manager URL and Resource Manager Port required for Hadoop Plugin")
        return

    collector = HadoopCollector(
        resource_manager_url=resource_manager_url,
        resource_manager_port=resource_manager_port,
        excluded_metrics=exclude,
        custom_dimensions=custom_dimensions,
        verbose=verbose,
    )

    collectd.register_read(collector.read_callback, interval=interval,
                           name='hadoop-'+resource_manager_url+resource_manager_port)


collectd.register_config(configure_callback)
