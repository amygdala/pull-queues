
"""This file contains some configuration variables for the app."""

# These queue names must be defined in queue.yaml
PULL_QUEUE_NAME = 'jobs'
WORKER_QUEUE_NAME = 'worker'
# For pull queue tagging, the number of different tags to use.
NUM_TAGS = 4
