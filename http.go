package jobstack

import "net/http"

type nop404 struct{}

// HTTPOptions extends the main stack options with the HTTP related configuration.
type HTTPOptions struct {

	// Options contains the common options for the stack.
	Options

	// StackFullStatusCode is used when a job needs to be dropped from the
	// stack before its processing has been started. Defaults to 503 Service
	// Unavailable.
	StackFullStatusCode int

	// TimeoutStatusCode is used when a job times out before its processing
	// has been started. Defaults to 503 Service Unavailable.
	TimeoutStatusCode int
}

// Handler is wrapper around Stack that implements the standard http.Handler
// interface.
type Handler struct {
	options HTTPOptions
	handler http.Handler
	stack   *Stack
}

func (nop404) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

// NewHandler initializes stack handler wrapping th ehttp.Handler argument.
// It uses the configured stack to control whether and when the processing of
// a request can be started. It limits the maximum number of requests that
// can be processed to the value of MaxConcurrency.
//
// Instances of the Handler needs to be closed with the Close method once
// they are not used anymore.
func NewHandler(o HTTPOptions, h http.Handler) *Handler {
	s := With(o.Options)
	if h == nil {
		h = nop404{}
	}

	if o.StackFullStatusCode == 0 {
		o.StackFullStatusCode = http.StatusServiceUnavailable
	}

	if o.TimeoutStatusCode == 0 {
		o.TimeoutStatusCode = http.StatusServiceUnavailable
	}

	return &Handler{options: o, stack: s, handler: h}
}

// ServeHTTP implements the http.Handler interface.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h.stack.Do(func() {
		h.handler.ServeHTTP(w, r)
	})

	switch err {
	case ErrStackFull:
		w.WriteHeader(h.options.StackFullStatusCode)
	case ErrTimeout:
		w.WriteHeader(h.options.TimeoutStatusCode)
	}
}

// Close frees up the resources used by a Handler instance.
func (h *Handler) Close() {
	h.stack.Close()
}
