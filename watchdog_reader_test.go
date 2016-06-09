// This tests WatchdogReader

package swift

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"
)

// Uses testReader from timeout_reader_test.go

func testWatchdogReaderTimeout(t *testing.T, initialTimeout, watchdogTimeout time.Duration, expectedTimeout bool) {
	test := newTestReader(3, 10*time.Millisecond)
	timer := time.NewTimer(initialTimeout)
	firedChan := make(chan bool)
	started := make(chan bool)
	go func() {
		started <- true
		select {
		case <-timer.C:
			firedChan <- true
		}
	}()
	<-started
	wr := newWatchdogReader(test, watchdogTimeout, timer)
	b, err := ioutil.ReadAll(wr)
	if err != nil || string(b) != "AAA" {
		t.Fatalf("Bad read %s %s", err, b)
	}
	fired := false
	select {
	case fired = <-firedChan:
	default:
	}
	if expectedTimeout {
		if !fired {
			t.Fatal("Timer should have fired")
		}
	} else {
		if fired {
			t.Fatal("Timer should not have fired")
		}
	}
}

func TestWatchdogReaderNoTimeout(t *testing.T) {
	testWatchdogReaderTimeout(t, 100*time.Millisecond, 100*time.Millisecond, false)
}

func TestWatchdogReaderTimeout(t *testing.T) {
	testWatchdogReaderTimeout(t, 5*time.Millisecond, 5*time.Millisecond, true)
}

func TestWatchdogReaderNoTimeoutShortInitial(t *testing.T) {
	testWatchdogReaderTimeout(t, 5*time.Millisecond, 100*time.Millisecond, false)
}

func TestWatchdogReaderTimeoutLongInitial(t *testing.T) {
	testWatchdogReaderTimeout(t, 100*time.Millisecond, 5*time.Millisecond, true)
}

func TestWatchdogReaderInternalReset(t *testing.T) {
	contents := "some text"
	in := bytes.NewBufferString(contents)
	timer := time.NewTimer(time.Second)
	wd := newWatchdogReader(in, time.Second, timer)

	// Initial state
	if wd.timeout != time.Second {
		t.Errorf("timeout wrong want 1s got %v", wd.timeout)
	}
	if wd.reader != in {
		t.Errorf("reader wrong want %v got %v", in, wd.timeout)
	}
	if wd.timer != timer {
		t.Errorf("timer wrong want %v got %v", timer, wd.timer)
	}
	if wd.bytes != 0 {
		t.Errorf("bytes wrong want %v got %v", 0, wd.bytes)
	}
	if wd.replayFirst != false {
		t.Errorf("replayFirst wrong want %v got %v", false, wd.replayFirst)
	}
	if wd.readFirst != false {
		t.Errorf("readFirst wrong want %v got %v", false, wd.readFirst)
	}
	if wd.first != 0 {
		t.Errorf("first wrong want %v got %v", 0, wd.readFirst)
	}

	// Check reset working
	if !wd.Reset() {
		t.Errorf("Expecting to be able to reset")
	}
	if wd.replayFirst != false {
		t.Errorf("replayFirst wrong want %v got %v", false, wd.replayFirst)
	}

	// Read one byte
	p := make([]byte, 1)
	n, err := wd.Read(p)
	if err != nil {
		t.Errorf("Not expecting err: %v", err)
	}
	if n != 1 {
		t.Errorf("Want to read 1 byte not %v", n)
	}
	if p[0] != 's' {
		t.Errorf("Want to read 's' byte not %q", p[0])
	}
	if wd.bytes != 1 {
		t.Errorf("bytes wrong want %v got %v", 1, wd.bytes)
	}
	if wd.replayFirst != false {
		t.Errorf("replayFirst wrong want %v got %v", false, wd.replayFirst)
	}
	if wd.readFirst != true {
		t.Errorf("readFirst wrong want %v got %v", true, wd.readFirst)
	}
	if wd.first != 's' {
		t.Errorf("first wrong want %v got %v", 's', wd.readFirst)
	}

	// Check reset working
	if !wd.Reset() {
		t.Errorf("Expecting to be able to reset")
	}
	if wd.replayFirst != true {
		t.Errorf("replayFirst wrong want %v got %v", true, wd.replayFirst)
	}

	// Read remainder
	all, err := ioutil.ReadAll(wd)
	if err != nil {
		t.Errorf("Not expecting err: %v", err)
	}
	if string(all) != contents {
		t.Errorf("Want to read %v but got %q", contents, all)
	}
	if err != nil {
		t.Errorf("Not expecting err: %v", err)
	}
	if n != 1 {
		t.Errorf("Want to read 1 byte not %v", n)
	}
	if p[0] != 's' {
		t.Errorf("Want to read 's' byte not %q", p[0])
	}
	if wd.bytes != int64(len(contents)) {
		t.Errorf("bytes wrong want %v got %v", len(contents), wd.bytes)
	}
	if wd.replayFirst != false {
		t.Errorf("replayFirst wrong want %v got %v", false, wd.replayFirst)
	}
	if wd.readFirst != true {
		t.Errorf("readFirst wrong want %v got %v", true, wd.readFirst)
	}
	if wd.first != 's' {
		t.Errorf("first wrong want %v got %v", 's', wd.readFirst)
	}

	// Check reset working
	if wd.Reset() {
		t.Errorf("Expecting not to be able to reset")
	}
	if wd.replayFirst != false {
		t.Errorf("replayFirst wrong want %v got %v", false, wd.replayFirst)
	}
}
