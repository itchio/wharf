package wtest

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
)

// Must shows a complete error stack and fails a test immediately
// if err is non-nil
func Must(t *testing.T, err error) {
	if err != nil {
		t.Helper()
		t.Errorf("%+v", errors.WithStack(err))
		if os.Getenv("MUST_SLEEPS") == "1" {
			fmt.Printf("Asked to sleep forever after: %+v\n", errors.WithStack(err))
			fmt.Printf("Sleeping forever...")
			for {
				time.Sleep(1 * time.Second)
			}
		}
		t.FailNow()
	}
}
