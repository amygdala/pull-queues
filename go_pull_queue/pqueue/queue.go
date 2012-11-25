/*
Task queue helper functions.
*/
package pqueue

import (
	"net/http"
	"net/url"
	"time"

	"appengine"
	"appengine/taskqueue"
)

// NewPULLTask creates a new pull task with the given parameters and tag.
func NewPULLTask(payload url.Values, tag string) *taskqueue.Task {
	h := make(http.Header)
	h.Set("Content-Type", "application/x-www-form-urlencoded")
	return &taskqueue.Task{
		Payload: []byte(payload.Encode()),
		Header:  h,
		Method:  "PULL",
		Tag:     tag,
	}
}

// PurgeQueue purges the given queue of tasks
func PurgeQueue(c appengine.Context, queueName string) {
	taskqueue.Purge(c, queueName)
	c.Infof("purging the %s queue...", queueName)
	// Queue purging has a resolution of one second. Allowing for up to a second
	// of clock skew between individual servers, it should be safe to add new
	// tasks after a two second wait.
	time.Sleep(time.Duration(2) * time.Second)
}
