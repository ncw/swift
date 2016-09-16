// +build go1.6

package swift

import (
	"net/http"
	"time"
)

const IS_AT_LEAST_GO_16 = true

func SetExpectContinueTimeout(tr *http.Transport, t time.Duration) {
	tr.ExpectContinueTimeout = t
}
