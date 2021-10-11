package discovery

import "sync"

// GetFindMutex A modified version of a RWMutex. Many get locks can be held but
// only one Find lock. A waiting Find lock (even if it hasn't been locked, just
// if someone is waiting) blocks all other get locks until it unlocks.
//
// The intended usage of this is that it will allow a source which is trying to
// process many requests at once, to process a FIND request before any GET
// requests, since it's likely that once FIND has been run, subsequent GET
// requests will be able to be served from cache
type GetFindMutex struct {
	mutex sync.RWMutex
}

// GetLock Gets a lock that can be held by an unlimited number of goroutines,
// these locks are only blocked by FindLocks
func (g *GetFindMutex) GetLock() {
	g.mutex.RLock()
}

// GetUnlock Unlocks the GetLock. This must be called once for each GetLock
// otherwise it will be impossible to ever obtain a FindLock
func (g *GetFindMutex) GetUnlock() {
	g.mutex.RUnlock()
}

// FindLock An exclusive lock. Ensure that all GetLocks have been unlocked
// and stops any more from being obtained
func (g *GetFindMutex) FindLock() {
	g.mutex.Lock()
}

// FindUnlock Unlocks a FindLock
func (g *GetFindMutex) FindUnlock() {
	g.mutex.Unlock()
}
