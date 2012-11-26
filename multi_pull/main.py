#!/usr/bin/python

"""This example app adds tagged jobs to a pull queue, then consumes them from a
'worker' push queue."""

import datetime
import logging
import os
import random
import time
import traceback
import urllib

import webapp2

import config

from google.appengine.api import taskqueue
from google.appengine.ext.webapp import template


def IntClamp(v, low, high):
  """Clamps a value to the integer range [low, high] (inclusive)."""
  return max(int(low), min(int(v), int(high)))


def ParseRootParams(request):
  """Parses the fields of the root form.
  Returns a dict containing the form fields with default values possibly
  overridden."""
  params = {
      'total_batches': 100,
      'tasks_per_batch': 10,
      'workers': 2,
      'tasks_per_worker': 2,
      'lease_seconds': 10,
      'max_leases_per_task': 3}

  for k, v in params.iteritems():
    # Possibly replace default values.
    params[k] = request.get(k, v)

  params['tasks_per_batch'] = IntClamp(
      params['tasks_per_batch'], 1, 100)
  params['workers'] = IntClamp(params['workers'], 0, 1000)
  return params


class RootHandler(webapp2.RequestHandler):
  """Shows combo populate and start workers form."""

  def get(self):
    self.RenderTemplate('index.html', ParseRootParams(self.request))

  def post(self):
    params = ParseRootParams(self.request)
    populate_params = {
        'total_batches': params['total_batches'],
        'tasks_per_batch': params['tasks_per_batch']}
    q = taskqueue.Queue('populator')
    q.add(taskqueue.Task(url='/populate', params=populate_params))
    self.redirect('/?' + urllib.urlencode(params))

  def RenderTemplate(self, name, template_args):
    """Renders a named django template."""
    path = os.path.join(os.path.dirname(__file__), 'templates', name)
    self.response.out.write(template.render(path, template_args))


class PopulateHandler(webapp2.RequestHandler):
  """Adds pull tasks to the jobs queue."""

  def post(self):
    """Adds tasks to the jobs queue.
    """

    tasks_per_batch = int(self.request.get('tasks_per_batch'))
    total_batches = int(self.request.get('total_batches'))

    tasks = []
    for _ in range(total_batches):
      tasks.append(
          taskqueue.Task(url='/add_tasks',
                         method='POST',
                         params={'tasks_per_batch': tasks_per_batch}))
    taskqueue.Queue('populator').add(tasks)


class AddTasksHandler(webapp2.RequestHandler):
  """Adds tasks to the pull queue."""

  def post(self):

    tag_id = 'tag_%i' % random.randrange(config.NUM_TAGS)
    tasks_per_batch = int(self.request.get('tasks_per_batch'))
    jobs = taskqueue.Queue(config.PULL_QUEUE_NAME)
    payload_string = 'This is a payload with tag [%s]' % tag_id
    jobs.add([taskqueue.Task(payload='task %d: %s' % (i, payload_string),
                             method='PULL', tag=tag_id)
              for i in range(tasks_per_batch)])


class WorkHandler(webapp2.RequestHandler):
  """Leases tasks from the jobs queue, possibly deleting the task."""

  def post(self):
    """Leases jobs and deletes with probability (retry_count / max_leases)."""
    lease_seconds = float(self.request.get('lease_seconds'))
    num_tasks = int(self.request.get('num_tasks'))
    max_leases = int(self.request.get('max_leases_per_task'))
    q = taskqueue.Queue(config.PULL_QUEUE_NAME)
    starttime = datetime.datetime.now()
    try:
      tag_id = 'tag_%i' % random.randrange(config.NUM_TAGS)
      logging.info('leasing for tag: %s', tag_id)
      tasks = q.lease_tasks_by_tag(lease_seconds, num_tasks, tag=tag_id)

      # Now, as an example, check jobs queue stats
      stats = taskqueue.QueueStatistics.fetch(config.PULL_QUEUE_NAME)
      logging.info('jobs stats: %s', stats)
      num_jobs = stats.tasks
      # for pull queues, the executed_last_minute value
      # indicates the number of tasks leased in the last minute.
      leased_last_minute = stats.executed_last_minute
      logging.info('number of jobs in the queue: %s; leased in last minute %s',
                   num_jobs, leased_last_minute)
      for t in tasks:
        logging.debug('task payload: %s', t.payload)
        # We might delete this task. We certainly will delete it after we've
        # leased it max_leases times, or if there are > 1000 tasks currently
        # reported to be in the queue.
        if num_jobs > 1000 or (random.randint(0, max_leases) < t.retry_count):
          q.delete_tasks(t)
    except:
      logging.exception(
          'lease exception for lease started at: %s',
          starttime)
    # Cause a non-logging failure so that this worker can reschedule.
    self.redirect('/')  # returns 302 status


class PurgeWorkQueueHandler(webapp2.RequestHandler):
  """Purge the worker queue."""

  def post(self):
    root_params = ParseRootParams(self.request)
    q = taskqueue.Queue(config.WORKER_QUEUE_NAME)
    q.purge()
    time.sleep(2)
    self.redirect('/?' + urllib.urlencode(root_params))


class StartHandler(webapp2.RequestHandler):
  """Starts some worker tasks."""

  def post(self):
    """Purges the worker queue then repopulates it after a safe interval."""
    root_params = ParseRootParams(self.request)
    q = taskqueue.Queue(config.WORKER_QUEUE_NAME)
    q.purge()
    # Queue purging has a resolution of one second. Allowing for up to a second
    # of clock skew between individual servers, it should be safe to add new
    # tasks after a two second wait.
    time.sleep(2)

    worker_params = {
        'lease_seconds': float(root_params['lease_seconds']),
        'num_tasks': int(root_params['tasks_per_worker']),
        'max_leases_per_task': int(root_params['max_leases_per_task'])
    }
    worker_retry_options = taskqueue.TaskRetryOptions(min_backoff_seconds=0.5,
                                                      max_backoff_seconds=0.5)
    workers = root_params['workers']
    while workers > 0:
      num_tasks = min(100, workers)
      q.add([taskqueue.Task(url='/work',
                            params=worker_params,
                            retry_options=worker_retry_options,
                           ) for _ in xrange(num_tasks)])
      workers -= num_tasks
    self.redirect('/?' + urllib.urlencode(root_params))

application = webapp2.WSGIApplication([
    ('/', RootHandler),
    ('/populate', PopulateHandler),
    ('/add_tasks', AddTasksHandler),
    ('/work', WorkHandler),
    ('/purge_workers', PurgeWorkQueueHandler),
    ('/start', StartHandler),
], debug=True)

