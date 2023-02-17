package discovery

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestThrottle(t *testing.T) {
	t.Run("can be unlocked and lock the expected number of times", func(t *testing.T) {
		throttle := NewThrottle(4)

		ctx, cancel := context.WithTimeout(context.Background(), (1 * time.Second))
		doneChan := make(chan bool)

		go func(t *testing.T) {
			throttle.Lock()
			throttle.Lock()
			throttle.Lock()
			throttle.Lock()

			if l := len(throttle.permissionChan); l != 0 {
				t.Errorf("permissionChan should be empty, got %v", l)
			}

			throttle.Unlock()
			throttle.Unlock()
			throttle.Unlock()
			throttle.Unlock()

			doneChan <- true
		}(t)

		select {
		case <-ctx.Done():
			t.Error("Timeout")
		case <-doneChan:
		}

		cancel()
	})
}

func TestWaiting(t *testing.T) {
	throttle := NewThrottle(4)

	var workComplete time.Time
	var newWorkStarted time.Time
	var wg sync.WaitGroup

	throttle.Lock()
	throttle.Lock()
	throttle.Lock()
	throttle.Lock()

	wg.Add(2)

	go func() {
		defer wg.Done()

		time.Sleep(100 * time.Millisecond)
		workComplete = time.Now()

		throttle.Unlock()
		throttle.Unlock()
		throttle.Unlock()
		throttle.Unlock()
	}()

	go func() {
		defer wg.Done()

		throttle.Lock()
		newWorkStarted = time.Now()
		defer throttle.Unlock()
	}()

	wg.Wait()

	if newWorkStarted.Before(workComplete) {
		t.Fatalf("New work was started before throttle was unlocked")
	}
}
