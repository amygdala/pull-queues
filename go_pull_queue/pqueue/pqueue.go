/*
Small pull queue demo.  First populate a pull queue, constructing task tags based
on a given prefix, then start some push queue workers to consume the pull queue tasks.
A worker will delete a leased task after it has been leased a given
number of times.  The workers reschedule themselves when they finish processing a lease, and
look for more work to do.
(After there are no more tasks in the pull queue to process, you can manually purge the
worker queue if you like).
*/
package pqueue

import (
	"fmt"
	"html/template"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"appengine"
	"appengine/taskqueue"
)

const (
	workerQueue       = "worker"    // the name of the worker push task queue
	jobQueue          = "jobs"      // the name of the pull queue
	populatorQueue    = "populator" // the name of the push queue used for the 'pull queue populator' tasks
	numTags           = 4           // number of tag variants to create given a tag prefix
	defaultNumWorkers = 5           // default number of worker tasks.
	maxBatchAdds      = 100         // This is currently the hard limit for batch task queue adds; do not increase this value.
	numPullTasks      = 50          // number of pull queue tasks to create in a batch.  Should not be > maxBatchAdds,
	// the batch limit.
	maxBackoff  = time.Duration(500) * (time.Millisecond) // The maximum backoff time between task retries
	minBackoff  = time.Duration(500) * (time.Millisecond) // The minimum backoff time between task retries
	maxToLease  = 3                                       // max number of tags to lease in a lease request
	leasePeriod = 20
	leaseLimit  = int32(2) // The number of leases on a task before it will be deleted
)

var (
	rt = &taskqueue.RetryOptions{ // The task retry options used for this application.
		MinBackoff: minBackoff,
		MaxBackoff: maxBackoff,
	}
)

func init() {
	http.HandleFunc("/", root)
	http.HandleFunc("/work", work)
	http.HandleFunc("/workers/start", startWorkers)
	http.HandleFunc("/workers/purge", purgeWorkers)
	http.HandleFunc("/populate", populate)
}

// populate creates and adds some pull queue tasks using the given tag
func populate(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	if tag := r.FormValue("tag"); tag != "" {
		payload := url.Values{"tagname": {tag}}
		c.Infof("Populating pull queue using tag %s", tag)
		tasks := make([]*taskqueue.Task, numPullTasks)
		for i := 0; i < numPullTasks; i++ {
			tasks[i] = NewPULLTask(payload, tag)
		}
		// do a batch add of the tasks
		if _, err := taskqueue.AddMulti(c, tasks, jobQueue); err != nil {
			c.Errorf("%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	// OK
}

// startWorkers starts some worker tasks
func startWorkers(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	PurgeQueue(c, workerQueue)
	tagprefix := r.FormValue("tagprefix")
	// determine the number of workers to use
	numWorkers, err := strconv.Atoi(r.FormValue("num_workers"))
	if err != nil {
		numWorkers = defaultNumWorkers
	}
	if numWorkers > maxBatchAdds {
		numWorkers = maxBatchAdds
	}
	c.Infof("starting %d workers", numWorkers)
	tasks := make([]*taskqueue.Task, numWorkers)
	// create the worker tasks
	for i := range tasks {
		var t *taskqueue.Task
		payload := url.Values{}
		if tagprefix != "" {
			payload.Set("tagprefix", tagprefix)
		}
		t = taskqueue.NewPOSTTask("/work", payload)
		// Set task retry backoff options - this is relevant because the workers will 
		// deliberately reschedule themselves after they process their lease.
		t.RetryOptions = rt
		tasks[i] = t
	}
	// add the worker tasks to the worker queue
	if _, err := taskqueue.AddMulti(c, tasks, workerQueue); err != nil {
		c.Errorf("%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/", http.StatusFound)
}

// The purgeWorkers handler purges the work queue.
func purgeWorkers(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	PurgeQueue(c, workerQueue)
	http.Redirect(w, r, "/", http.StatusFound)

}

// The root handler spawns some pull queue populator tasks.
// Each populator task will add some tasks to the pull queue, tagged with a tag
// based on the given prefix.
func root(w http.ResponseWriter, r *http.Request) {
	var msg string
	c := appengine.NewContext(r)

	if r.Method == "POST" {
		tagprefix := "mytag"
		if p := r.FormValue("tagprefix"); p != "" {
			tagprefix = p
		}
		for i := 0; i < numTags; i++ {
			// generate a tag based on the given tag prefix
			tag := fmt.Sprintf("%s_%d", tagprefix, i)
			// add a populator task to the queue, which will create a set of pull queue
			// tasks using that tag.
			t := taskqueue.NewPOSTTask("/populate", url.Values{"tag": {tag}})
			if _, err := taskqueue.Add(c, t, populatorQueue); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			msg = fmt.Sprintf("Populator tasks started, using prefix '%s'.", tagprefix)
		}
	}

	if err := handlerTemplate.Execute(w, msg); err != nil {
		c.Errorf("%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	// OK
}

// The work handler leases some tasks from a pull queue, grouped by tag, and
// deletes the leased tasks if they have been leased a given number of times.
func work(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	tag := ""
	if p := r.FormValue("tagprefix"); p != "" {
		// randomly generate a tag that the worker will lease against
		tag = fmt.Sprintf("%s_%d", p, rand.Intn(numTags))
	}

	// Lease some tasks.
	// The LeaseByTag function leases by tag group. 
	// If tagId == "" then tasks with any tag will be leased.
	tasks, err := taskqueue.LeaseByTag(c, maxToLease, jobQueue, leasePeriod, tag)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, t := range tasks {
		payload, err := url.ParseQuery(string(t.Payload))
		if err != nil {
			c.Errorf("%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tagname := payload["tagname"]
		c.Infof("task name: %s, tagname %s, tag: %s", t.Name, tagname, t.Tag)
		// if this task has been leased leaseLimit times...
		if t.RetryCount >= leaseLimit {
			// then delete the pull task from the queue
			c.Infof("deleting: %s", t.Name)
			err := taskqueue.Delete(c, t, jobQueue)
			if err != nil {
				c.Warningf("%v", err)
			}
		}
	}
	// Cause a non-logging failure so that this worker can reschedule.
	http.Redirect(w, r, "/", http.StatusFound)
}

var handlerTemplate = template.Must(template.New("handler").Parse(handlerHTML))

// handlerHTML is the root template
const handlerHTML = `
<!doctype HTML>
<html>
<body>
	{{with .}}<p>{{.}}</p>{{end}}
	<p>Launch some populator tasks, which will add pull tasks to a job queue:</p>
	<form action="/" method="POST">
		optional tag prefix: <input type="text" name="tagprefix">
		<input type="submit" value="Add">
	</form>
	<hr/>
	<p>Start some worker tasks to consume the job queue:</p>
	<form action="/workers/start">
		optional tag prefix to filter on: <input type="text" name="tagprefix"><br/>
		number of workers: <input type="text" name="num_workers"><br/>
		<input type="submit" value="Start">
	</form>
	<hr/>
	<form action="/workers/purge">
		Purge the workers queue:
		<input type="submit" value="Purge Workers">
	</form>
</body>
</html>
`
