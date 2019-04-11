/*
JobStack

This library provides stack implementation for long running or otherwise expensive processing jobs. As a
special case, it implements the standard http.Handler in addition to the generic interface.

Mechanism

The stack defines a maximum concurrency limit at which the jobs can be executed, and makes them wait if this
limit is exceeded. The default concurrency limit is 1. It is important to note, that the stack doesn't start
individual goroutines for each job, the jobs have to have their own goroutines and the stack should be called
from those.

Besides limiting the concurrency level, it is also possible to limit the number of pending jobs, either by
setting the maximum stack size, or a timeout for the jobs, or both.

Example

	func processJobs(jobs []func()) (dropped, timedOut int) {
		stack := jobstack.With(Options{
			MaxConcurrency: 256,
			MaxStackSize:   256 * 256,
			Timeout:        9 * time.Millisecond,
		})

		for _, j := range jobs {
			go func(j func()) {
				err := stack.Do(j)
				switch err {
				case jobstack.ErrStackFull:
					dropped++
				case jobstack.ErrTimeout:
					timedOut++
				}
			}(j)
		}

		return
	}

Two-step example

	func processInSharedStack(s *jobstack.Stack, job func()) error {
		done, err := s.Ready()
		if err != nil {
			return err
		}

		job()
		done()
		return nil
	}
*/
package jobstack
