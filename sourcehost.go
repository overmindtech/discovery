package discovery

import (
	"sort"
	"sync"
)

// SourceHost This struct holds references to all Sources in a process
// and provides utility functions to work with them. Methods of this
// struct are safe to call concurrently.
type SourceHost struct {
	// Map of types to all sources for that type
	sourceMap      map[string][]Source
	sourceMapMutex sync.RWMutex
}

func NewSourceHost() *SourceHost {
	return &SourceHost{
		sourceMap: make(map[string][]Source),
	}
}

// AddSources Adds a source to this engine
func (sh *SourceHost) AddSources(sources ...Source) {
	sh.sourceMapMutex.Lock()
	defer sh.sourceMapMutex.Unlock()

	for _, src := range sources {
		allSources := append(sh.sourceMap[src.Type()], src)

		sort.Slice(allSources, func(i, j int) bool {
			iSource := allSources[i]
			jSource := allSources[j]

			// Sort by weight, highest first
			return iSource.Weight() > jSource.Weight()
		})

		sh.sourceMap[src.Type()] = allSources
	}
}

// Sources Returns a slice of all known sources
func (sh *SourceHost) Sources() []Source {
	sh.sourceMapMutex.RLock()
	defer sh.sourceMapMutex.RUnlock()

	sources := make([]Source, 0)

	for _, typeSources := range sh.sourceMap {
		sources = append(sources, typeSources...)
	}

	return sources
}

// VisibleSources Returns a slice of all known sources excluding hidden ones
func (sh *SourceHost) VisibleSources() []Source {
	allSources := sh.Sources()
	result := make([]Source, 0)

	// Add all sources unless they are hidden
	for _, source := range allSources {
		if hs, ok := source.(HiddenSource); ok {
			if hs.Hidden() {
				// If the source is hidden, continue without adding it
				continue
			}
		}

		result = append(result, source)
	}

	return result
}

// Sources Returns a slice of all known sources with a specific type
func (sh *SourceHost) SourcesByType(typ string) []Source {
	sh.sourceMapMutex.RLock()
	defer sh.sourceMapMutex.RUnlock()

	if sources, ok := sh.sourceMap[typ]; ok {
		result := make([]Source, len(sources))
		copy(result, sources)
		return result
	}

	return make([]Source, 0)
}
