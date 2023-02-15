package discovery

import "sync"

// Throttle limits the number of processes that can be executing at once to
// `numParallel`. Users should call `Lock()` to obtain a lock and `Unlock()`
// once their work is done
type Throttle struct {
	numParallel    int
	permissionChan chan bool
	setupMutex     sync.Mutex
}

// NewThrottle Creates a new throttle that allows only `numParallel` request to
// run at once. If numParallel is < 1 it will be set to 1
func NewThrottle(numParallel int) *Throttle {
	if numParallel < 1 {
		numParallel = 1
	}

	t := Throttle{
		numParallel:    numParallel,
		permissionChan: make(chan bool, numParallel),
		setupMutex:     sync.Mutex{},
	}

	// Populate the channel
	for i := 0; i < numParallel; i++ {
		t.permissionChan <- true
	}

	return &t
}

func (t *Throttle) Lock() {
	<-t.permissionChan
}

func (t *Throttle) Unlock() {
	// Check that we are not unlocking beyond the original buffer length as this will hang forever
	if len(t.permissionChan) == int(t.numParallel) {
		panic("attempt to unlock already fully unlocked Throttle")
	}

	t.permissionChan <- true
}
