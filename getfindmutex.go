package discovery

import (
	"fmt"
	"sync"
)

// GetFindMutex A modified version of a RWMutex. Many get locks can be held but
// only one Find lock. A waiting Find lock (even if it hasn't been locked, just
// if someone is waiting) blocks all other get locks until it unlocks.
//
// The intended usage of this is that it will allow a source which is trying to
// process many requests at once, to process a FIND request before any GET
// requests, since it's likely that once FIND has been run, subsequent GET
// requests will be able to be served from cache
type GetFindMutex struct {
	mutexMap map[string]*sync.RWMutex
	mapLock  sync.Mutex
}

// GetLock Gets a lock that can be held by an unlimited number of goroutines,
// these locks are only blocked by FindLocks. A type and context must be
// provided since a Get in one type (or context) should not be blocked by a Find
// in another
func (g *GetFindMutex) GetLock(itemContext string, typ string) {
	g.mutexFor(itemContext, typ).RLock()
}

// GetUnlock Unlocks the GetLock. This must be called once for each GetLock
// otherwise it will be impossible to ever obtain a FindLock
func (g *GetFindMutex) GetUnlock(itemContext string, typ string) {
	g.mutexFor(itemContext, typ).RUnlock()
}

// FindLock An exclusive lock. Ensure that all GetLocks have been unlocked and
// stops any more from being obtained. Provide a type and context to ensure that
// the lock is only help for that type and context combination rather than
// locking the whole engine
func (g *GetFindMutex) FindLock(itemContext string, typ string) {
	g.mutexFor(itemContext, typ).Lock()
}

// FindUnlock Unlocks a FindLock
func (g *GetFindMutex) FindUnlock(itemContext string, typ string) {
	g.mutexFor(itemContext, typ).Unlock()
}

// mutexFor Returns the relevant RWMutex for a given context and type, creating
// and storing a new one if needed
func (g *GetFindMutex) mutexFor(itemContext string, typ string) *sync.RWMutex {
	var mutex *sync.RWMutex
	var ok bool

	keyName := g.keyName(itemContext, typ)

	g.mapLock.Lock()
	defer g.mapLock.Unlock()

	// Create the map if needed
	if g.mutexMap == nil {
		g.mutexMap = make(map[string]*sync.RWMutex)
	}

	// Get the mutex from storage
	mutex, ok = g.mutexMap[keyName]

	// If the mutex wasn't found for this key, create a new one
	if !ok {
		mutex = &sync.RWMutex{}
		g.mutexMap[keyName] = mutex
	}

	return mutex
}

// keyName Returns the name of the key for a given context and type combo for
// use with the mutexMap
func (g *GetFindMutex) keyName(itemContext string, typ string) string {
	return fmt.Sprintf("%v.%v", itemContext, typ)
}
