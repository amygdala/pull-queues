#!/usr/bin/python

"""Demonstrates use of the pull queue's REST API and service accounts to read
and process jobs from a pull queue of a different app engine app."""

from __future__ import with_statement

import base64
import logging
import os
import random
import time
import urllib

import jinja2
import webapp2

from apiclient.discovery import build
import config
import httplib2
from oauth2client.appengine import AppAssertionCredentials

from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.api import users

jinja_environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)))


class MainPage(webapp2.RequestHandler):
  """Allow the user to stop and start some workers, which consume jobs from a
  remote pull queue owned by another app engine app."""

  def get(self):
    """Connect to the remote queue and display some stats on it."""

    msg = self.request.get('msg')
    queue_name = config.JOB_QUEUE
    project = 's~%s' % (config.PROVIDER_APP_NAME,)
    try:
      credentials = AppAssertionCredentials(
          scope='https://www.googleapis.com/auth/taskqueue')
      http = credentials.authorize(httplib2.Http(memcache))
      task_api = build('taskqueue', 'v1beta2', http=http,
                       developerKey=config.developerKey)
      req = task_api.taskqueues().get(project=project,
                                      taskqueue=queue_name,
                                      getStats=True)
      taskqueue_info = req.execute()
      stats = taskqueue_info['stats']
      leased_last_minute = stats['leasedLastMinute']
      leased_last_hour = stats['leasedLastHour']
      total_tasks = stats['totalTasks']
      template_vars = {'msg': msg, 'totalTasks': total_tasks,
                       'leasedLastMinute': leased_last_minute,
                       'leasedLastHour': leased_last_hour,
                       'queue_name': queue_name}
    except:
      logging.exception('error accessing remote pull queue.')
      self.response.write('Error accessing remote pull queue.')
      self.response.write("""<br>Note that this app won't run correctly
          unless deployed, and must first be properly configured.
          See the README for the app for more information.""")
      return
    template = jinja_environment.get_template('static/index.html')
    self.response.write(template.render(template_vars))


class PurgeWorkQueueHandler(webapp2.RequestHandler):
  """Purge the worker queue."""

  def post(self):
    q = taskqueue.Queue(config.WORKER_QUEUE_NAME)
    q.purge()
    time.sleep(2)
    params = {'msg': 'Purged workers...'}
    self.redirect('/?' + urllib.urlencode(params))


class StartHandler(webapp2.RequestHandler):
  """Starts some worker tasks."""

  # For this simple demo, we're hardwiring the number of worker tasks.
  NUM_WORKERS = 25

  def post(self):
    """Purges the worker queue, then repopulates it after a safe interval."""

    q = taskqueue.Queue(config.WORKER_QUEUE_NAME)
    q.purge()

    # Queue purging has a resolution of one second. Allowing for up to a second
    # of clock skew between individual servers, it should be safe to add new
    # tasks after a two second wait.
    time.sleep(2)

    # For this simple demo, we're hardwiring the lease config.
    lease_seconds = 20
    tasks_per_worker = 10
    max_leases_per_task = 3

    tlist = []
    user = users.get_current_user()
    worker_retry_options = taskqueue.TaskRetryOptions(min_backoff_seconds=0.5,
                                                      max_backoff_seconds=0.5)
    for _ in range(self.NUM_WORKERS):
      tag_id = 'tag_%i' % random.randrange(config.NUM_TAGS)
      worker_params = {
          'lease_seconds': lease_seconds,
          'num_tasks': tasks_per_worker,
          'max_leases_per_task': max_leases_per_task,
          'tag_id': tag_id,
          'user_id': user.user_id()
      }

      tlist.append(taskqueue.Task(url='/get_remote',
                                  params=worker_params,
                                  retry_options=worker_retry_options
                                 ))
    q.add(tlist)
    params = {'msg': 'Started workers...'}
    self.redirect('/?' + urllib.urlencode(params))


class GetRemoteTasks(webapp2.RequestHandler):
  """Leases tagged tasks from the remote jobs queue, deleting the tasks."""

  NUM_TAGS = 4

  def getNextTasks(self, lease_seconds, num_tasks, tag_id):
    """Pull next task(s) off the jobs queue."""

    queue_name = config.JOB_QUEUE
    credentials = (
        AppAssertionCredentials(
            scope='https://www.googleapis.com/auth/taskqueue'))
    http = credentials.authorize(httplib2.Http(memcache))
    task_api = build('taskqueue', 'v1beta2',
                     http=http, developerKey=config.developerKey)

    try:
      project = 's~%s' % (config.PROVIDER_APP_NAME,)
      lease_req = task_api.tasks().lease(project=project,
                                         taskqueue=queue_name,
                                         groupByTag=True, tag=tag_id,
                                         leaseSecs=lease_seconds,
                                         numTasks=num_tasks)
      res = lease_req.execute()
      res['queue_name'] = queue_name
      logging.debug('Got lease result: %s', res)
      return res
    except:
      logging.exception('Problem leasing tasks')

  def deleteCompletedTask(self, res, item):
    credentials = (
        AppAssertionCredentials(
            scope='https://www.googleapis.com/auth/taskqueue'))
    http = credentials.authorize(httplib2.Http(memcache))
    task_api = build('taskqueue', 'v1beta2',
                     http=http, developerKey=config.developerKey)

    task_api.tasks().delete(taskqueue=res['queue_name'],
                            project='s~%s' % config.PROVIDER_APP_NAME,
                            task=item['id']).execute()

  def post(self):
    """Run by a worker task.
    Lease some tasks from the remote pull queue, then delete each one
    after processing."""

    lease_seconds = float(self.request.get('lease_seconds'))
    num_tasks = int(self.request.get('num_tasks'))
    tag_id = self.request.get('tag_id')
    logging.info('Tag id: %s', tag_id)

    res = self.getNextTasks(lease_seconds, num_tasks, tag_id)
    if res:
      try:
        items = res['items']
        # if we have task items
        try:
          for item in items:
            task = base64.b64decode(item['payloadBase64'])
            # here, you'd typically do something with the task...
            # ... then we delete it.
            logging.debug('Deleting task: %s', item)
            self.deleteCompletedTask(res, item)
        except:
          logging.exception('Problem processing or deleting tasks')
      except:
        logging.info('No task queue items returned.')
    # Cause a non-logging failure so that this worker can reschedule.
    self.redirect('/')


app = webapp2.WSGIApplication([('/', MainPage),
                               ('/workers/purge', PurgeWorkQueueHandler),
                               ('/workers/start', StartHandler),
                               ('/get_remote', GetRemoteTasks),
                              ],
                              debug=True)
