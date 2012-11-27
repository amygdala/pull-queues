
"""..."""

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
import appengine_credentials
import config
import httplib2
from oauth2client.client import OAuth2WebServerFlow

from google.appengine.api import taskqueue
from google.appengine.api import users
from google.appengine.ext import db

TASK_SCOPE = 'https://www.googleapis.com/auth/taskqueue'
FLOW = OAuth2WebServerFlow(
    client_id=config.client_id,
    client_secret=config.client_secret,
    scope=[TASK_SCOPE],
    user_agent='app-engine-demo/1.0',
    access_type='offline')

jinja_environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)))


def get_auth_http(credentials):
  """Get the Compute service."""
  http = httplib2.Http()
  http = credentials.authorize(http)
  return http


class Credentials(db.Model):
  credentials = appengine_credentials.CredentialsProperty()


class MainPage(webapp2.RequestHandler):
  """Show main page instances."""

  def get(self):
    """..."""

    msg = self.request.get('msg')
    user = users.get_current_user()
    credentials = appengine_credentials.StorageByKeyName(
        Credentials, user.user_id(), 'credentials').get()

    if credentials and not credentials.invalid:
      try:
        queue_name = config.JOB_QUEUE
        project = 's~%s' % (config.PROVIDER_APP_NAME,)
        auth_http = get_auth_http(credentials)
        task_api = build('taskqueue', 'v1beta2', http=auth_http)
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
        template = jinja_environment.get_template('static/index.html')
        self.response.write(template.render(template_vars))
      except:
        logging.exception('error accessing remote pull queue.')
        self.response.write('Error accessing remote pull queue.')
    else:
      callback = self.request.relative_url('/oauth2callback')
      template_vars = {'url': FLOW.step1_get_authorize_url(callback)}
      template = jinja_environment.get_template('static/login.html')
      self.response.write(template.render(template_vars))


class OAuthHandler(webapp2.RequestHandler):
  """Handle OAuth 2.0 redirect."""

  def get(self):
    user = users.get_current_user()
    credentials = FLOW.step2_exchange(self.request.params)
    appengine_credentials.StorageByKeyName(
        Credentials, user.user_id(), 'credentials').put(credentials)
    self.redirect('/')


class PurgeWorkQueueHandler(webapp2.RequestHandler):
  """...."""

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

    # For this simple demo, we'll just hardwire the lease information.
    lease_seconds = 20
    tasks_per_worker = 10
    max_leases_per_task = 3
    tlist = []
    user = users.get_current_user()

    worker_retry_options = taskqueue.TaskRetryOptions(
        min_backoff_seconds=0.5,
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

      tlist.append(taskqueue.Task(
          url='/get_remote',
          params=worker_params,
          retry_options=worker_retry_options))
    q.add(tlist)
    params = {'msg': 'Started workers...'}
    self.redirect('/?' + urllib.urlencode(params))


class GetRemoteTasks(webapp2.RequestHandler):
  """Leases tagged tasks from the remote jobs queue, deleting the task."""

  def getNextTasks(self, lease_seconds, num_tasks, tag_id, user_id):
    """Pull next task(s) off the jobs queue."""

    self.credentials = appengine_credentials.StorageByKeyName(
        Credentials, user_id, 'credentials').get()
    logging.debug('got credentials: %s', self.credentials)
    queue_name = config.JOB_QUEUE
    auth_http = get_auth_http(self.credentials)
    task_api = build('taskqueue', 'v1beta2', http=auth_http)

    try:
      project = 's~%s' % (config.PROVIDER_APP_NAME,)
      lease_req = task_api.tasks().lease(
          project=project,
          taskqueue=queue_name,
          groupByTag=True, tag=tag_id,
          leaseSecs=lease_seconds,
          numTasks=num_tasks,
          )
      res = lease_req.execute()
      res['queue_name'] = queue_name
      logging.debug('Got lease result: %s', res)
      return res
    except:
      logging.exception('Problem leasing tasks')

  def pushCompletedTask(self, res, item, user_id):

    self.credentials = appengine_credentials.StorageByKeyName(
        Credentials, user_id, 'credentials').get()
    auth_http = get_auth_http(self.credentials)
    task_api = build('taskqueue', 'v1beta2', http=auth_http)

    task_api.tasks().delete(
        taskqueue=res['queue_name'],
        project='s~%s' % config.PROVIDER_APP_NAME,
        task=item['id']).execute()

  def post(self):
    """..."""

    lease_seconds = float(self.request.get('lease_seconds'))
    num_tasks = int(self.request.get('num_tasks'))
    tag_id = self.request.get('tag_id')
    user_id = self.request.get('user_id')
    logging.debug('tag id: %s, user_id %s', tag_id, user_id)

    res = self.getNextTasks(lease_seconds, num_tasks, tag_id, user_id)
    if res:
      try:
        items = res['items']
        # if we have task items
        try:
          for item in items:
            task = base64.b64decode(item['payloadBase64'])
            # here, you'd typically do something with the task...
            # ... then we delete it.
            logging.debug('deleting task now: %s', item)
            self.pushCompletedTask(res, item, user_id)
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
                               ('/oauth2callback', OAuthHandler),
                              ],
                              debug=True)
