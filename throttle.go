package discovery

// Throttle limits the number of processes that can be executing at once to
// `NumParallel`. Users should call `Lock()` to obtain a lock and `Unlock()`
// once their work is done
type Throttle struct {
	NumParallel int

	permissionChan chan bool
}

func (t *Throttle) Lock() {
	t.ensureSetup()

	<-t.permissionChan
}

func (t *Throttle) Unlock() {
	t.ensureSetup()

	// Check that we are not unlocking beyond the original buffer length as this will hang forever
	if len(t.permissionChan) == int(t.NumParallel) {
		panic("attempt to unlock already fully unlocked Throttle")
	}

	t.permissionChan <- true
}

// ensureSetup ensures that the underlying chan setup is initialised. It also
// ensures that NumParallel is set to at least 1
func (t *Throttle) ensureSetup() {
	if t.NumParallel < 1 {
		t.NumParallel = 1
	}

	if t.permissionChan == nil {
		t.permissionChan = make(chan bool, t.NumParallel)

		// Populate the channel
		for i := 0; i < t.NumParallel; i++ {
			t.permissionChan <- true
		}
	}
}
