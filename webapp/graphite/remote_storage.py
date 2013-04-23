import socket
import time
import httplib
import requests
from urlparse import urlparse
from urllib import urlencode
from django.core.cache import cache
from django.conf import settings
from graphite.render.hashing import compactHash
from graphite.logger import log
from statsd import statsd


try:
  import cPickle as pickle
except ImportError:
  import pickle


class RemoteStore(object):
  lastFailure = 0.0
  retryDelay = settings.REMOTE_STORE_RETRY_DELAY
  available = property(lambda self: time.time() - self.lastFailure > self.retryDelay)

  def __init__(self, url_or_host):
    if url_or_host.startswith('http://') or url_or_host.startswith('https://'):
      self.url = url_or_host
    else:
      self.url = 'http://{0}/'.format(url_or_host)
    self.host = urlparse(self.url).hostname
    log.info('Added remote store: ' + self.url)


  def find(self, query):
    statsd.incr('{0}.find.req'.format(self.get_statsd_key()))
    request = FindRequest(self, query)
    request.send()
    return request


  def fail(self):
    self.lastFailure = time.time()


  def get_statsd_key(self):
    return 'remote.{0}'.format(self.host.replace('.', '_'))

  def __unicode__(self):
    return '<RemoteStore: {0}>'.format(self.url)



class FindRequest:
  r = None
  suppressErrors = True

  def __init__(self, store, query):
    self.store = store
    self.query = query
    self.connection = None
    self.cacheKey = compactHash('find:%s:%s' % (self.store.host, query))
    self.cachedResults = None


  def send(self):
    self.cachedResults = cache.get(self.cacheKey)

    if self.cachedResults:
      return

    query_params = [
      ('local', '1'),
      ('format', 'pickle'),
      ('query', self.query),
    ]
    query_string = urlencode(query_params)
    url = '{0}/metrics/find/'.format(self.store.url)

    try:
      key = '{0}.find.duration'.format(self.store.get_statsd_key())
      with statsd.timer(key):
        self.r = requests.get(url, params=dict(query_params), timeout=settings.REMOTE_STORE_FIND_TIMEOUT)
    except Exception, e:
      statsd.incr('{0}.find.error.connection'.format(self.store.get_statsd_key()))
      log.info('Error fetching data from store {0}: {1}'.format(self.store.url, str(e)))
      self.store.fail()
      if not self.suppressErrors:
        raise


  def get_results(self):
    if self.cachedResults:
      statsd.incr('{0}.find.cache_hit'.format(self.store.get_statsd_key()))
      return self.cachedResults
    statsd.incr('{0}.find.cache_miss'.format(self.store.get_statsd_key()))

    if not self.r:
      self.send()

    if self.r:
      assert self.r.status_code == 200, "received error response %s - %s" % (r.status_code, r.text)
      try:
        result_data = self.r.content
        results = pickle.loads(result_data)
      except:
        statsd.incr('{0}.find.error.bad_response'.format(self.store.get_statsd_key()))
        self.store.fail()
        if not self.suppressErrors:
          raise
        else:
          results = []
    else:
      results = []

    resultNodes = [ RemoteNode(self.store, node['metric_path'], node['isLeaf']) for node in results ]
    cache.set(self.cacheKey, resultNodes, settings.REMOTE_FIND_CACHE_DURATION)
    self.cachedResults = resultNodes
    return resultNodes



class RemoteNode:
  context = {}

  def __init__(self, store, metric_path, isLeaf):
    self.store = store
    self.fs_path = None
    self.metric_path = metric_path
    self.real_metric = metric_path
    self.name = metric_path.split('.')[-1]
    self.__isLeaf = isLeaf


  def fetch(self, startTime, endTime):
    statsd.incr('{0}.get.req'.format(self.store.get_statsd_key()))
    if not self.__isLeaf:
      return []

    query_params = [
      ('target', self.metric_path),
      ('format', 'pickle'),
      ('from', str( int(startTime) )),
      ('until', str( int(endTime) ))
    ]

    url = '{0}/render/'.format(self.store.url)
    try:
      with statsd.timer('{0}.get.duration'.format(self.store.get_statsd_key())):
        r = requests.get(url, params=dict(query_params), timeout=settings.REMOTE_STORE_FETCH_TIMEOUT)
    except:
      statsd.incr('{0}.get.error.connection'.format(self.store.get_statsd_key()))
      log.info('Failed fetching {0} from {1}').format(self.metric_path, self.store.url)
      raise

    try:
      assert r.status_code == 200, "Failed to retrieve remote data: %d %s" % (r.status_code, r.text)
    except:
      statsd.incr('{0}.get.error.bad_response'.format(self.store.get_statsd_key()))
      raise
    rawData = r.content

    seriesList = pickle.loads(rawData)
    try:
      assert len(seriesList) == 1, "Invalid result: seriesList=%s" % str(seriesList)
    except:
      statsd.incr('{0}.get.error.invalid_result'.format(self.store.get_statsd_key()))
      raise

    series = seriesList[0]

    timeInfo = (series['start'], series['end'], series['step'])
    return (timeInfo, series['values'])


  def isLeaf(self):
    return self.__isLeaf

