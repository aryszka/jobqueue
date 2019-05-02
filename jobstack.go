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
	quit    chan struct{}
	busy    int
}

var token struct{}

var (
	// ErrStackFull is returned by the stack when the max stack size is reached.
	ErrStackFull = errors.New("stack is full")

	// ErrTimeout is returned by the stack when a pending job reached the timeout.
	ErrTimeout = errors.New("timeout")
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
		quit:    make(chan struct{}),
	}

	go s.run()
	return s
}

func (s *Stack) run() {
	for {
		var timeout <-chan time.Time
		oldest := s.stack.bottom()
		if oldest != nil {
			timeout = oldest.timeout
		}

		select {
		case j := <-s.req:
			if s.busy < s.options.MaxConcurrency {
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
		case <-timeout:
			oldest.notify <- ErrTimeout
			s.stack.shift()
		case <-s.quit:
			// TODO: teardown
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
	s.req <- j
	err = <-j.notify
	done = func() { s.done <- token }
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

// Close frees up the resources used by a Stack instance.
func (s *Stack) Close() {
	close(s.quit)
}

// MoveTo moves all stacks from the current to the new stack. The
// current stack should be already closed.
func (s *Stack) MoveTo(newStack *Stack) {
	for s.busy > 0 && newStack.busy < newStack.stack.cap {
		j := s.stack.pop()
		s.busy--
		if j != nil {
			newStack.stack.push(j)
			newStack.busy++
		}
	}
}
