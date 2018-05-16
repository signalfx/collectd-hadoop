import collectd
import metrics
import urllib2
import json
import re

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

HTTP_404_ERROR = "Error 404"


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


def _dimensions_str_to_dict(dimensions_str):
    """
    convert str config of dimensions into dictionary
    Args:
    dimensions_str (str): String representing custom dimensions
    """

    dimensions = {}
    dimensions_list = dimensions_str.strip().split(',')

    for dimension in dimensions_list:
        kv = dimension.strip().split('=')
        if _validate_kv(kv):
            dimensions[kv[0]] = kv[1]
        else:
            collectd.info("Could not validate key-val in dimensions \
             from configuration file.")
    return dimensions


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
                 dimensions=None, timestamp=None):
        self.name = metric_name
        self.type = metric_type
        self.value = value
        self.dimensions = dimensions or {}

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
        emit_value.plugin_instance = '{0}'.format(
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
                 excluded_metrics, custom_dimensions):
        self.metric_sink = MetricSink()
        self.resource_manager_url = resource_manager_url
        self.resource_manager_port = resource_manager_port
        self.excluded_metrics = excluded_metrics
        self.custom_dimensions = custom_dimensions
        collectd.info("Successfully configured Hadoop Collector ...")

    def get_json_from_rest_request(self, url, path, *args, **kwargs):
        """
        Makes REST call and converts response to JSON
        """

        resp = None
        resp = self.rest_request(url, path, *args, **kwargs)
        if not resp:
            return []

        try:
            return json.loads(resp)
        except ValueError, e:
            collectd.info("Error parsing JSON from API call (%s) %s/%s" %
                          (e, url, path))
            return []

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
            if HTTP_404_ERROR not in str(e):
                collectd.info("Unable to make request at (%s) %s" % (e, url))
            return None
        except:
            return None

    def read_callback(self):
        """
        Makes API calls for cluster metrics
        and posts them to SignalFx
        """
        rm_url = self.resource_manager_url+":"+self.resource_manager_port

        rm_metrics = self.get_cluster_metrics(rm_url)
        self.post_metrics(rm_metrics)

        scheduler_metrics = self.get_scheduler_metrics(rm_url)
        self.post_metrics(scheduler_metrics)

        apps_metrics = self.get_apps_metrics(rm_url)
        self.post_metrics(apps_metrics)

        node_metrics = self.get_node_metrics(rm_url)
        self.post_metrics(node_metrics)

        mapreduce_metrics = self.get_mapreduce_metrics(rm_url)
        self.post_metrics(mapreduce_metrics)

    def get_cluster_metrics(self, rm_url):
        rm_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_METRICS_PATH)
        if not rm_resp:
            return []

        dim = {}
        dim.update(self.custom_dimensions)
        metric_records = []
        for cluster_metric, cm_value in rm_resp.get('clusterMetrics').iteritems():
            for key, (metric_type, metric_name) in metrics.HADOOP_CLUSTER_METRICS.iteritems():
                if key not in cluster_metric or metric_name in self.excluded_metrics:
                    continue
                metric_records.append(MetricRecord(metric_name, metric_type, cm_value, dim))

        return metric_records

    def get_node_metrics(self, rm_url):
        nodes_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_NODES_PATH)
        if not nodes_resp or nodes_resp is None:
            return []

        metric_records = []
        nodes = nodes_resp['nodes']['node']

        for node in nodes:
            dim = {"node_rack": node['rack'], "state": node['state'], "node_id": node['id']}
            dim.update(self.custom_dimensions)
            for key, (metric_type, metric_name) in metrics.HADOOP_NODE_METRICS.iteritems():
                if key not in node or metric_name in self.excluded_metrics:
                    continue

                metric_value = node[key]
                metric_records.append(MetricRecord(metric_name, metric_type, metric_value, dim))

        return metric_records

    def get_apps_metrics(self, rm_url):
        apps_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_APPS_PATH,
                                                    states="accepted,running")

        if not apps_resp or apps_resp['apps'] is None:
            return []

        metric_records = []
        apps = apps_resp['apps']['app']

        for app in apps:
            dim = {"app_name": app['name'], "queue_name": app['queue'], "user": app['user'],
                   "applicationType": app['applicationType']}
            dim.update(self.custom_dimensions)
            for key, (metric_type, metric_name) in metrics.HADOOP_APPLICATIONS.iteritems():
                if key not in app or metric_name in self.excluded_metrics:
                    continue

                metric_value = app[key]
                metric_records.append(MetricRecord(metric_name, metric_type, metric_value, dim))

        return metric_records

    def get_mapreduce_metrics(self, rm_url):
        running_apps = self.get_running_mapreduce_apps(rm_url)
        jobs_metrics = self.get_jobs_from_apps(running_apps)
        return jobs_metrics

    def get_running_mapreduce_apps(self, rm_url):
        apps_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_APPS_PATH,
                                                    states="running", applicationTypes=MAPREDUCE_APP_TYPE)

        if not apps_resp or apps_resp['apps'] is None:
            return {}

        running_apps = {}
        apps = apps_resp['apps']['app']
        for app in apps:
            app_id = app['id']
            app_name = app['name']
            app_url = app['trackingUrl']

            if app_id and app_name and app_url:
                running_apps[app_id] = (app_name, app_url)

        return running_apps

    def get_jobs_from_apps(self, running_apps):
        metric_records = []

        for app_id, (app_name, trackingUrl) in running_apps.iteritems():
            jobs_resp = self.get_json_from_rest_request(trackingUrl, MAPREDUCE_PATH, MAPREDUCE_JOBS_PATH)
            if not jobs_resp or jobs_resp['jobs'] is None:
                continue
            jobs = jobs_resp['jobs']['job']
            for job in jobs:
                dim = {"user": job['user'], "job_id": job['id'], "job_name": job['name'], "app_id": app_id,
                       "app_name": app_name}
                dim.update(self.custom_dimensions)

                for key, (metric_type, metric_name) in metrics.MAPREDUCE_JOB_METRICS.iteritems():
                    if key not in job or metric_name in self.excluded_metrics:
                        continue

                    metric_value = job[key]
                    metric_records.append(MetricRecord(metric_name, metric_type, metric_value, dim))

        return metric_records

    def get_scheduler_metrics(self, rm_url):
        scheduler_resp = self.get_json_from_rest_request(rm_url, RESOURCE_MANAGER_ENDPOINT, CLUSTER_SCHEDULER_PATH)
        if not scheduler_resp:
            return []

        metric_records = []

        def sub_queues(queue):
            return queue.get('queues', {}).get('queue', [])

        def get_queue_metric(queue):
            queue_type = queue.get('type')
            queue_metric = None

            if queue_type == 'capacitySchedulerLeafQueueInfo':
                queue_metric = metrics.HADOOP_LEAF_QUEUE
                dim = {"queue_name": (queue.get('queueName')), "queue_state": (queue.get('state')),
                       "queue_type": queue_type}
            elif queue_type == 'capacityScheduler':
                queue_metric = metrics.HADOOP_ROOT_QUEUE
                dim = {"queue_name": (queue.get('queueName')), "queue_type": queue_type}
            elif queue_type == 'fifoScheduler':
                queue_metric = metrics.HADOOP_FIFO_SCHEDULER
            elif queue_type is None:
                queue_metric = metrics.HADOOP_LEAF_QUEUE
                dim = {"queue_name": (queue.get('queueName')), "queue_state": (queue.get('state')),
                       "queue_type": 'capacitySchedulerLeafQueueInfo'}

            dim.update(self.custom_dimensions)

            if queue_type is not None:
                for cluster_metric, cm_value in queue.iteritems():
                    for key, (metric_type, metric_name) in queue_metric.iteritems():
                        if key not in cluster_metric or metric_name in self.excluded_metrics:
                            continue
                        metric_records.append(MetricRecord(metric_name, metric_type, cm_value, dim))

        def traverse(node, do_work, children_of):

            def work(node, continue_working, depth=0):
                if continue_working:
                    result = do_work(node)
                    depth = depth
                    if continue_working and not result:
                        continue_working = False
                        return
                    for child in children_of(node):
                        work(child, depth + 1)
            work(node, True)

        scheduler_queues = scheduler_resp['scheduler']['schedulerInfo']
        traverse(scheduler_queues, get_queue_metric, sub_queues)
        return metric_records

    def post_metrics(self, metrics):
        for metric in metrics:
            self.metric_sink.emit(metric)


def configure_callback(conf):
    """
    A callback method that  loads information from the Hadoop collectd plugin config file.
    Args:
    config_values (collectd.Config): Object containing config values
    """
    collectd.info("Configuring hadoop plugins")

    resource_manager_url = None
    resource_manager_port = None
    exclude = set()
    interval = None
    custom_dimensions = {}

    for node in conf.children:
        if node.key == 'ResourceManagerURL' and node.values[0]:
            if not _validate_url(node.values[0]):
                raise ValueError("URL is not prefixed with http://")
            resource_manager_url = node.values[0]
        elif node.key == 'ResourceManagerPort' and node.values[0]:
            resource_manager_port = str(int(node.values[0]))
        elif node.key == 'Interval' and node.values[0]:
            interval = node.values[0]
        elif node.key == 'Dimension':
            if len(node.values) == 2:
                custom_dimensions.update({node.values[0]: node.values[1]})
            else:
                collectd.warning("WARNING: Check configuration \
                                            setting for %s" % node.key)
        elif node.key == 'ExcludeMetrics' and node.values[0]:
            _add_metrics_to_set(exclude, node.values[0])

    if resource_manager_url is None or resource_manager_port is None:
        collectd.error("Resource Manager URL and Resource Manager Port required for Hadoop Plugin")
        return

    collector = HadoopCollector(**{
        'resource_manager_url': resource_manager_url,
        'resource_manager_port': resource_manager_port,
        'excluded_metrics': exclude,
        'custom_dimensions': custom_dimensions
    })

    collectd.register_read(collector.read_callback, interval=interval, name='hadoop-'+resource_manager_url)


collectd.register_config(configure_callback)
