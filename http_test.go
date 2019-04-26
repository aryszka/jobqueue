package jobqueue

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type tserver struct {
	handler       *Handler
	testingServer *httptest.Server
	url           string
}

type testHandler struct {
	counter jobCounter
}

func testServer(o HTTPOptions, h http.Handler) *tserver {
	s := NewHandler(o, h)
	ts := httptest.NewServer(s)
	return &tserver{
		handler:       s,
		testingServer: ts,
		url:           ts.URL,
	}
}

func (ts *tserver) close() {
	ts.handler.Close()
	ts.testingServer.Close()
}

func (h *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d, _ := time.ParseDuration(r.Header.Get("X-Sleep"))
	h.counter.do(d)
	w.Write([]byte("Hello, world!"))
}

func testGetSlow(u string, d time.Duration) (statusCode int, body string, err error) {
	var (
		req *http.Request
		rsp *http.Response
		b   []byte
	)

	req, err = http.NewRequest("GET", u, nil)
	if err != nil {
		return
	}

	if d > 0 {
		req.Header.Set("X-Sleep", d.String())
	}

	rsp, err = http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}

	defer rsp.Body.Close()
	b, err = ioutil.ReadAll(rsp.Body)
	statusCode, body = rsp.StatusCode, string(b)
	return
}

func testGet(u string) (statusCode int, body string, err error) {
	return testGetSlow(u, 0)
}

func mustGetSlow(t *testing.T, u string, d time.Duration) (statusCode int, body string) {
	var err error
	statusCode, body, err = testGetSlow(u, d)
	if err != nil {
		t.Fatal(err)
	}

	return
}

func mustGet(t *testing.T, u string) (statusCode int, body string) {
	return mustGetSlow(t, u, 0)
}

func TestNop404Handler(t *testing.T) {
	s := testServer(HTTPOptions{Options: Options{MaxConcurrency: 1}}, nil)
	defer s.close()
	c, _ := mustGet(t, s.url)
	if c != http.StatusNotFound {
		t.Error("unexpected status code received", c, "expected", http.StatusNotFound)
	}
}

func TestBasicServe(t *testing.T) {
	s := testServer(HTTPOptions{Options: Options{MaxConcurrency: 1}}, &testHandler{})
	defer s.close()
	c, b := mustGet(t, s.url)
	if c != http.StatusOK {
		t.Error("unexpected status code", c, "expected", http.StatusOK)
	}

	if b != "Hello, world!" {
		t.Error("unexpected response content", b, "expected", "Hello, world!")
	}
}

func TestServeSetMaxConcurrency(t *testing.T) {
	h := &testHandler{}
	s := testServer(HTTPOptions{Options: Options{MaxConcurrency: 3}}, h)
	defer s.close()

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			mustGetSlow(t, s.url, 9*time.Millisecond)
			wg.Done()
		}()
	}

	wg.Wait()
	if h.counter.maxJobs != 3 {
		t.Errorf("failed to limit the max concurrent jobs. Observed: %d, expected %d", h.counter.maxJobs, 3)
	}
}

func TestServeCancel(t *testing.T) {
	t.Run("stack full", func(t *testing.T) {
		s := testServer(HTTPOptions{Options: Options{MaxConcurrency: 3, MaxStackSize: 2}}, &testHandler{})
		defer s.close()
		var wg sync.WaitGroup
		results := make(chan int, 6)
		wg.Add(6)
		for i := 0; i < 6; i++ {
			go func() {
				c, _ := mustGetSlow(t, s.url, 9*time.Millisecond)
				results <- c
				wg.Done()
			}()
		}

		wg.Wait()
		close(results)
		var found bool
		for r := range results {
			if found && r != http.StatusOK {
				t.Error(r)
				continue
			}

			if r == http.StatusOK {
				continue
			}

			if r == http.StatusServiceUnavailable {
				found = true
				continue
			}

			t.Errorf("invalid result: %v", r)
		}

		if !found {
			t.Error("failed to receive error")
		}
	})

	t.Run("timeout", func(t *testing.T) {
		s := testServer(HTTPOptions{Options: Options{Timeout: 3 * time.Millisecond}}, &testHandler{})
		defer s.close()
		var wg sync.WaitGroup
		results := make(chan int, 2)
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				c, _ := mustGetSlow(t, s.url, 18*time.Millisecond)
				results <- c
				wg.Done()
			}()
		}

		wg.Wait()
		close(results)
		var found bool
		for r := range results {
			if found && r != http.StatusOK {
				t.Error(r)
				continue
			}

			if r == http.StatusOK {
				continue
			}

			if r == http.StatusServiceUnavailable {
				found = true
				continue
			}

			t.Errorf("invalid result: %v", r)
		}

		if !found {
			t.Error("failed to receive error")
		}
	})
}

func TestThrottlingOptions(t *testing.T) {
	// status code for stack size
	// status code for timeout
	// custom headers for stack size
	// custom headers for throttling
}
