package jobqueue

import (
	"sync"
	"testing"
	"time"
)

type jobCounter struct {
	mx                  sync.Mutex
	activeJobs, maxJobs int
}

func (c *jobCounter) do(d time.Duration) {
	func() {
		c.mx.Lock()
		defer c.mx.Unlock()
		c.activeJobs++
		if c.activeJobs > c.maxJobs {
			c.maxJobs = c.activeJobs
		}
	}()

	defer func() {
		c.mx.Lock()
		defer c.mx.Unlock()
		c.activeJobs--
	}()

	time.Sleep(d)
}

func TestSingleJob(t *testing.T) {
	w := With(Options{MaxConcurrency: 1, MaxStackSize: 1})
	defer w.Close()
	if err := w.Do(func() {}); err != nil {
		t.Error(err)
	}
}

func TestDefaultConcurrency(t *testing.T) {
	w := New()
	defer w.Close()
	if err := w.Do(func() {}); err != nil {
		t.Error(err)
	}
}

func TestSetMaxConcurrency(t *testing.T) {
	w := With(Options{MaxConcurrency: 3, MaxStackSize: 6})
	defer w.Close()
	c := &jobCounter{}
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			if err := w.Do(func() {
				c.do(9 * time.Millisecond)
			}); err != nil {
				t.Error(err)
			}

			wg.Done()
		}()
	}

	wg.Wait()
	if c.maxJobs != 3 {
		t.Errorf("failed to limit the max concurrent jobs. Observed: %d, expected %d", c.maxJobs, 3)
	}
}

func TestCancel(t *testing.T) {
	t.Run("stack full", func(t *testing.T) {
		w := With(Options{MaxConcurrency: 3, MaxStackSize: 2})
		defer w.Close()
		var wg sync.WaitGroup
		results := make(chan error, 6)
		wg.Add(6)
		for i := 0; i < 6; i++ {
			go func() {
				results <- w.Do(func() {
					time.Sleep(9 * time.Millisecond)
				})

				wg.Done()
			}()
		}

		wg.Wait()
		close(results)
		var found bool
		for r := range results {
			if found && r != nil {
				t.Error(r)
				continue
			}

			if r == nil {
				continue
			}

			if r == ErrStackFull {
				found = true
				continue
			}

			t.Errorf("invalid result: %v", r)
		}

		if !found {
			t.Error("failed to receive stack-full")
		}
	})

	t.Run("timeout", func(t *testing.T) {
		w := With(Options{Timeout: time.Millisecond})
		defer w.Close()
		var wg sync.WaitGroup
		results := make(chan error, 2)
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				results <- w.Do(func() {
					time.Sleep(9 * time.Millisecond)
				})

				wg.Done()
			}()
		}

		wg.Wait()
		close(results)
		var found bool
		for r := range results {
			if found && r != nil {
				t.Error(r)
				continue
			}

			if r == nil {
				continue
			}

			if r == ErrTimeout {
				found = true
				continue
			}

			t.Errorf("invalid result: %v", r)
		}

		if !found {
			t.Error("failed to receive timeout")
		}
	})
}

func TestTeardown(t *testing.T) {
	t.Run("call after closed", func(t *testing.T) {
		q := New()
		q.Close()
		<-q.hasQuit
		_, err := q.Wait()
		if err != ErrClosed {
			t.Fail()
		}
	})

	t.Run("call after closed while busy", func(t *testing.T) {
		q := New()
		done, err := q.Wait()
		if err != nil {
			t.Fatal(err)
		}

		defer done()
		q.Close()
		_, err = q.Wait()
		if err != ErrClosed {
			t.Error("failed to report closed")
		}
	})

	t.Run("jobs get processed", func(t *testing.T) {
		q := New()
		completeJobs := make(chan struct{})
		for i := 0; i < 3; i++ {
			go func() {
				done, err := q.Wait()
				if err != nil {
					t.Error(err)
					return
				}

				<-completeJobs
				done()
			}()
		}

		for {
			s := q.Status()
			if s.Active+s.Queued == 3 {
				break
			}
		}

		q.Close()
		close(completeJobs)
		<-q.hasQuit
	})

	t.Run("teardown timeout", func(t *testing.T) {
		q := With(Options{CloseTimeout: 12 * time.Millisecond})

		_, err := q.Wait()
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				_, err := q.Wait()
				if err != ErrClosed {
					t.Error("failed to fail with ErrClosed")
				}

				wg.Done()
			}()
		}

		for {
			s := q.Status()
			if s.Active+s.Queued == 3 {
				break
			}
		}

		q.Close()
		wg.Wait()
	})
}

func TestForcedTeardown(t *testing.T) {
	t.Run("queued jobs get canceled", func(t *testing.T) {
		q := New()

		_, err := q.Wait()
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				_, err := q.Wait()
				if err != ErrClosed {
					t.Error("failed to fail with ErrClosed")
				}

				wg.Done()
			}()
		}

		for {
			s := q.Status()
			if s.Active+s.Queued == 3 {
				break
			}
		}

		q.CloseForced()
		wg.Wait()
	})

	t.Run("processed jobs done is a noop", func(t *testing.T) {
		q := New()
		done, err := q.Wait()
		if err != nil {
			t.Fatal(err)
		}

		q.CloseForced()
		<-q.hasQuit
		done()
	})

	t.Run("forced close after normal close", func(t *testing.T) {
		q := New()

		_, err := q.Wait()
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				_, err := q.Wait()
				if err != ErrClosed {
					t.Error("failed to fail with ErrClosed")
				}

				wg.Done()
			}()
		}

		for {
			s := q.Status()
			if s.Active+s.Queued == 3 {
				break
			}
		}

		q.Close()
		q.Status() // call status to make sure that we entered the control loop
		q.CloseForced()
		wg.Wait()
	})
}
