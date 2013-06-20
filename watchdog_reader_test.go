// This tests WatchdogReader

package swift

import (
	"io/ioutil"
	"testing"
	"time"
)

// Uses testReader from timeout_reader_test.go

func TestWatchdogReaderNoTimeout(t *testing.T) {
	test := newTestReader(3, 10*time.Millisecond)
	timer := time.NewTimer(100 * time.Millisecond)
	fired := false
	go func() {
		select {
		case <-timer.C:
			fired = true
		}
	}()
	wr := newWatchdogReader(test, 100*time.Millisecond, timer)
	b, err := ioutil.ReadAll(wr)
	if err != nil || string(b) != "AAA" {
		t.Fatalf("Bad read %s %s", err, b)
	}
	if fired {
		t.Fatal("Timer should not have fired")
	}
}

func TestWatchdogReaderTimeout(t *testing.T) {
	test := newTestReader(3, 10*time.Millisecond)
	timer := time.NewTimer(5 * time.Millisecond)
	fired := false
	go func() {
		select {
		case <-timer.C:
			fired = true
		}
	}()
	wr := newWatchdogReader(test, 5*time.Millisecond, timer)
	b, err := ioutil.ReadAll(wr)
	if err != nil || string(b) != "AAA" {
		t.Fatalf("Bad read %s %s", err, b)
	}
	if !fired {
		t.Fatal("Timer should have fired")
	}
}
