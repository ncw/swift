package swift

import (
	"io"
	"time"
)

// An io.ReadCloser which obeys an idle timeout
type timeoutReader struct {
	reader  io.ReadCloser
	timeout time.Duration
	cancel  func()
}

// Returns a wrapper around the reader which obeys an idle
// timeout. The cancel function is called if the timeout happens
func newTimeoutReader(reader io.ReadCloser, timeout time.Duration, cancel func()) *timeoutReader {
	return &timeoutReader{
		reader:  reader,
		timeout: timeout,
		cancel:  cancel,
	}
}

// Read reads up to len(p) bytes into p
//
// Waits at most for timeout for the read to complete otherwise returns a timeout
func (t *timeoutReader) Read(p []byte) (n int, err error) {
	// FIXME limit the amount of data read in one chunk so as to not exceed the timeout?
	// Do the read in the background
	done := make(chan bool, 1)
	go func() {
		n, err = t.reader.Read(p)
		done <- true
	}()
	// Wait for the read or the timeout
	select {
	case <-done:
		return
	case <-time.After(t.timeout):
		t.cancel()
		return 0, TimeoutError
	}
	return // for Go 1.0
}

// Close the channel
func (t *timeoutReader) Close() error {
	return t.reader.Close()
}

// Check it satisfies the interface
var _ io.ReadCloser = &timeoutReader{}
