package jobqueue

import (
	"container/list"
	"errors"
	"time"
)

type job struct {
	notify  chan error
	timeout <-chan time.Time
	entry   *list.Element
}

// Options allows passing in parameters to the stack.
type Options struct {

	// MaxConcurrency defines how many jobs are allowed to run concurrently.
	// Defaults to 1.
	MaxConcurrency int

	// MaxStackSize defines how many jobs may be waiting in the stack.
	// Defaults to infinite.
	MaxStackSize int

	// Timeout defines how long a job can be waiting in the stack.
	// Defaults to infinite.
	Timeout time.Duration

	// CloseTimeout sets a maximum duration for how long the queue can wait
	// for the active and queued jobs to finish. Defaults to infinite.
	CloseTimeout time.Duration
}

// Status contains snapshot information about the state of the queue.
type Status struct {

	// Active contains the number of jobs being executed.
	ActiveJobs int

	// Queued contains the number of jobs waiting to be scheduled.
	QueuedJobs int

	// Closing indicates that the queue is being closed.
	Closing bool

	// Closed indicates that the queues has been closed.
	Closed bool
}

// Stack controls how long running or otherwise expensive jobs are executed. It allows
// the jobs to proceed with limited concurrency. The incoming jobs are executed in LIFO
// style (Last-in-first-out).
//
// Jobs also can be dropped or timed out, when the MaxStackSize and/or Timeout options
// are set. When MaxStackSize is reached, the oldest job is dropped.
//
// Using a stack for job processing can be a good way to protect an application from
// bursts of chatty clients or temporarily slow job execution.
type Stack struct {
	options Options
	stack   *stack
	req     chan *job
	done    chan struct{}
	quit    chan bool
	closing bool
	status  chan chan Status
	hasQuit chan struct{}
	busy    int
}

var token struct{}

var (
	// ErrStackFull is returned by the stack when the max stack size is reached.
	ErrStackFull = errors.New("stack is full")

	// ErrTimeout is returned by the stack when a pending job reached the timeout.
	ErrTimeout = errors.New("timeout")

	// ErrClosed is returned by the queue when called after the queue was closed, or when the
	// queue was closed while a job was waiting to be scheduled.
	ErrClosed = errors.New("queue closed")
)

// New creates a Stack instance with a concurrency level of 1, and with infinite stack
// size and timeout. See With(Options), too. The Stack needs to be closed once it's not
// used anymore.
func New() *Stack {
	return With(Options{})
}

// With creates a Stack instance configured by the Options parameter. The Stack needs to
// be closed once it's not used anymore.
func With(o Options) *Stack {
	if o.MaxConcurrency <= 0 {
		o.MaxConcurrency = 1
	}

	s := &Stack{
		options: o,
		stack:   newStack(o.MaxStackSize),
		req:     make(chan *job),
		done:    make(chan struct{}),
		quit:    make(chan bool),
		hasQuit: make(chan struct{}),
		status:  make(chan chan Status),
	}

	go s.run()
	return s
}

func (s *Stack) rejectQueued() {
	for !s.stack.empty() {
		j := s.stack.shift()
		j.notify <- ErrClosed
	}
}

func (s *Stack) run() {
	var closeTimeout <-chan time.Time
	for {
		var timeout <-chan time.Time
		oldest := s.stack.bottom()
		if oldest != nil {
			timeout = oldest.timeout
		}

		select {
		case j := <-s.req:
			if s.closing {
				j.notify <- ErrClosed
			} else if s.busy < s.options.MaxConcurrency {
				s.busy++
				j.notify <- nil
			} else {
				if s.stack.full() {
					oldest := s.stack.shift()
					oldest.notify <- ErrStackFull
				}

				s.stack.push(j)
			}
		case <-s.done:
			s.busy--
			if !s.stack.empty() {
				s.busy++
				j := s.stack.pop()
				j.notify <- nil
			}

			if s.closing && s.busy == 0 && s.stack.empty() {
				close(s.hasQuit)
				return
			}
		case <-timeout:
			oldest.notify <- ErrTimeout
			s.stack.shift()
		case status := <-s.status:
			status <- Status{ActiveJobs: s.busy, QueuedJobs: s.stack.list.Len(), Closing: s.closing}
		case forced := <-s.quit:
			if forced {
				s.rejectQueued()
				close(s.hasQuit)
				return
			}

			s.closing = true
			if s.busy == 0 && s.stack.empty() {
				close(s.hasQuit)
				return
			}

			if s.options.CloseTimeout > 0 {
				closeTimeout = time.After(s.options.CloseTimeout)
			}
		case <-closeTimeout:
			s.rejectQueued()
			close(s.hasQuit)
			return
		}
	}
}

func (s *Stack) newJob() *job {
	j := &job{notify: make(chan error)}
	if s.options.Timeout > 0 {
		j.timeout = time.After(s.options.Timeout)
	}

	return j
}

// Wait returns when a job can be processed, or it should be cancelled. The notion of
// the actual 'job' to be processed is completely up to the calling code.
//
// When a job can be processed, Wait returns a non-nil done() function, which must be
// called after the job was done, in order to free-up a slot for the next job.
//
// When the job needs to be droppped, Wait returns ErrStackFull. When the job timed out,
// Wait returns ErrTimeout. In these cases, done() must not be called, and it may be
// nil.
//
// Wait doesn't return other errors than ErrStackFull or ErrTimeout.
func (s *Stack) Wait() (done func(), err error) {
	j := s.newJob()
	select {
	case s.req <- j:
		err = <-j.notify
		if err != nil {
			done = func() {}
		} else {
			done = func() {
				select {
				case s.done <- token:
				case <-s.hasQuit:
				}
			}
		}
	case <-s.hasQuit:
		err = ErrClosed
	}

	return
}

// Do calls the job, as soon as the number of the running jobs is not higher than the
// MaxConcurrency.
//
// If a job is dropped from the stack or times out, ErrStackFull or ErrTimeout is
// returned. Do does not return any other errors than ErrStackFull or ErrTimeout.
//
// Once the job has been started, Do does not return an error.
func (s *Stack) Do(job func()) error {
	done, err := s.Wait()
	if err != nil {
		return err
	}

	job()
	done()
	return nil
}

// Status returns snapshot information about the state of the queue.
func (s *Stack) Status() Status {
	req := make(chan Status)
	select {
	case <-s.hasQuit:
		return Status{Closed: true}
	case s.status <- req:
		return <-req
	}

}

// Close frees up the resources used by a Stack instance.
//
// After called, the queue stops accepting new jobs, but it waits until all the
// jobs are done, including those waiting in the queue.
//
// If the close timeout is set to >0, then forces closing after the timeout
// has passed. If the timeout has passed, the queued jobs receive ErrClosed.
// The close timeout can be set as an initialization option to the queue.
func (s *Stack) Close() {
	select {
	case <-s.hasQuit:
	case s.quit <- false:
	}
}

// CloseForced frees up the resources used by a Stack instance.
//
// When called, the queued jobs receive ErrClosed.
func (s *Stack) CloseForced() {
	select {
	case <-s.hasQuit:
	case s.quit <- true:
	}
}
