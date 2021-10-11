package discovery

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestGetLock(t *testing.T) {
	t.Run("many get locks can be held at once", func(t *testing.T) {
		var gfm GetFindMutex
		ctx, cancel := context.WithTimeout(context.Background(), (1 * time.Second))
		doneChan := make(chan bool)

		go func() {
			gfm.GetLock()
			gfm.GetLock()
			gfm.GetLock()
			gfm.GetUnlock()
			gfm.GetUnlock()
			gfm.GetUnlock()
			doneChan <- true
		}()

		select {
		case <-ctx.Done():
			t.Error("Timeout")
		case <-doneChan:
		}

		cancel()
	})

	t.Run("get locks are blocked by a find lock", func(t *testing.T) {
		var gfm GetFindMutex
		ctx, cancel := context.WithTimeout(context.Background(), (1 * time.Second))
		getChan := make(chan bool)
		findChan := make(chan bool)

		gfm.FindLock()

		go func() {
			gfm.GetLock()
			gfm.GetLock()
			gfm.GetLock()
			gfm.GetUnlock()
			gfm.GetUnlock()
			gfm.GetUnlock()
			getChan <- true
		}()

		go func() {
			// Seep for long enough to allow the above goroutine to complete if not
			// blocked
			time.Sleep(10 * time.Millisecond)

			findChan <- true
		}()

		select {
		case <-ctx.Done():
			t.Error("Timeout")
		case <-getChan:
			t.Error("Get locks were not blocked")
		case <-findChan:
			// This is the expected path
		}

		cancel()
	})

	t.Run("active gets block finds", func(t *testing.T) {
		var gfm GetFindMutex
		ctx, cancel := context.WithTimeout(context.Background(), (1 * time.Second))

		order := make([]string, 0)
		actionChan := make(chan string)
		doneChan := make(chan bool)
		var wg sync.WaitGroup
		wg.Add(3)

		go func() {
			defer wg.Done()
			gfm.GetLock()
			actionChan <- "getLock1"

			// do some work
			time.Sleep(50 * time.Millisecond)

			gfm.GetUnlock()

		}()

		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)

			gfm.FindLock()

			actionChan <- "findLock1"

			// do some work
			time.Sleep(50 * time.Millisecond)

			gfm.FindUnlock()

		}()

		go func() {
			defer wg.Done()
			time.Sleep(20 * time.Millisecond)

			gfm.GetLock()

			actionChan <- "getLock2"

			// do some work
			time.Sleep(50 * time.Millisecond)

			gfm.GetUnlock()

		}()

		go func() {
			for action := range actionChan {
				order = append(order, action)
			}
		}()

		go func(t *testing.T) {
			wg.Wait()

			// The expected order is: Firstly getLock1 since nothing else is waiting
			// for a lock. While this one is working there is a request for a
			// findlock, then a getlock. The findlock should block the getlock until
			// it is done
			if order[0] != "getLock1" {
				t.Errorf("expected getLock1 to be first. Order was: %v", order)
			}

			if order[1] != "findLock1" {
				t.Errorf("expected findLock1 to be middle. Order was: %v", order)
			}

			if order[2] != "getLock2" {
				t.Errorf("expected getLock2 to be last. Order was: %v", order)
			}

			doneChan <- true
		}(t)

		select {
		case <-ctx.Done():
			t.Errorf("timeout. Completed actions were: %v", order)
		case <-doneChan:
			// This is good
		}

		cancel()
	})
}
