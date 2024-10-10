package discovery

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/proto"
)

// AdapterHost This struct holds references to all Adapters in a process
// and provides utility functions to work with them. Methods of this
// struct are safe to call concurrently.
type AdapterHost struct {
	// Map of types to all adapters for that type
	adapterMap      map[string][]Adapter
	adapterMapMutex sync.RWMutex
}

func NewAdapterHost() *AdapterHost {
	sh := &AdapterHost{
		adapterMap: make(map[string][]Adapter),
	}

	// Add meta-adapters so that we can respond to queries for `overmind-type`,
	// `overmind-scope` and `overmind-adapter` resources
	sh.addBuiltinAdapters()

	return sh
}

func (sh *AdapterHost) addBuiltinAdapters() {
	sh.AddAdapters(&TypeAdapter{sh: sh})
	sh.AddAdapters(&ScopeAdapter{sh: sh})
	sh.AddAdapters(&SourcesAdapter{sh: sh})
}

// AddAdapters Adds an adapter to this engine
func (sh *AdapterHost) AddAdapters(adapters ...Adapter) {
	sh.adapterMapMutex.Lock()
	defer sh.adapterMapMutex.Unlock()

	for _, src := range adapters {
		allAdapters := append(sh.adapterMap[src.Type()], src)

		sort.Slice(allAdapters, func(i, j int) bool {
			iAdapter := allAdapters[i]
			jAdapter := allAdapters[j]

			// Sort by weight, highest first
			return iAdapter.Weight() > jAdapter.Weight()
		})

		sh.adapterMap[src.Type()] = allAdapters
	}
}

// Adapters Returns a slice of all known adapters
func (sh *AdapterHost) Adapters() []Adapter {
	sh.adapterMapMutex.RLock()
	defer sh.adapterMapMutex.RUnlock()

	adapters := make([]Adapter, 0)

	for _, typeAdapters := range sh.adapterMap {
		adapters = append(adapters, typeAdapters...)
	}

	return adapters
}

// VisibleAdapters Returns a slice of all known adapters excluding hidden ones
func (sh *AdapterHost) VisibleAdapters() []Adapter {
	allAdapters := sh.Adapters()
	result := make([]Adapter, 0)

	// Add all adapters unless they are hidden
	for _, adapter := range allAdapters {
		if hs, ok := adapter.(HiddenAdapter); ok {
			if hs.Hidden() {
				// If the adapter is hidden, continue without adding it
				continue
			}
		}

		result = append(result, adapter)
	}

	return result
}

// AdapterByType Returns all adapters of a given type
func (sh *AdapterHost) AdaptersByType(typ string) []Adapter {
	sh.adapterMapMutex.RLock()
	defer sh.adapterMapMutex.RUnlock()

	if adapters, ok := sh.adapterMap[typ]; ok {
		result := make([]Adapter, len(adapters))
		copy(result, adapters)
		return result
	}

	return make([]Adapter, 0)
}

// ExpandQuery Expands queries with wildcards to no longer contain wildcards.
// Meaning that if we support 5 types, and a query comes in with a wildcard
// type, this function will expand that query into 5 queries, one for each
// type.
//
// The same goes for scopes, if we have a query with a wildcard scope, and
// a single adapter that supports 5 scopes, we will end up with 5 queries. The
// exception to this is if we have a adapter that supports all scopes, but is
// unable to list them. In this case there will still be some queries with
// wildcard scopes as they can't be expanded
//
// This functions returns a map of queries with the adapters that they should be
// run against
func (sh *AdapterHost) ExpandQuery(q *sdp.Query) map[*sdp.Query][]Adapter {
	queries := make(map[string]*struct {
		Query    *sdp.Query
		Adapters []Adapter
	})

	var checkAdapters []Adapter

	if IsWildcard(q.GetType()) {
		// If the query has a wildcard type, all non-hidden adapters might try
		// to respond
		checkAdapters = sh.VisibleAdapters()
	} else {
		// If the type is specific, pull just adapters for that type
		checkAdapters = sh.AdaptersByType(q.GetType())
	}

	for _, src := range checkAdapters {
		// is the adapter is hidden
		isHidden := false
		if hs, ok := src.(HiddenAdapter); ok {
			isHidden = hs.Hidden()
		}

		for _, adapterScope := range src.Scopes() {
			// Create a new query if:
			//
			// * The adapter supports all scopes, or
			// * The query scope is a wildcard (and the adapter is not hidden), or
			// * The query scope substring matches adapter scope
			if IsWildcard(adapterScope) || (IsWildcard(q.GetScope()) && !isHidden) || strings.Contains(adapterScope, q.GetScope()) {
				dest := sdp.Query{}
				q.Copy(&dest)

				dest.Type = src.Type()

				// Choose the more specific scope
				if IsWildcard(adapterScope) {
					dest.Scope = q.GetScope()
				} else {
					dest.Scope = adapterScope
				}

				// deal with duplicate queries after expansion
				hash, err := queryHash(&dest)

				if err == nil {
					if existing, ok := queries[hash]; ok {
						existing.Adapters = append(existing.Adapters, src)
					} else {
						queries[hash] = &struct {
							Query    *sdp.Query
							Adapters []Adapter
						}{
							Query: &dest,
							Adapters: []Adapter{
								src,
							},
						}
					}
				}
			}
		}
	}

	// Convert back to final map
	finalMap := make(map[*sdp.Query][]Adapter)
	for _, expanded := range queries {
		finalMap[expanded.Query] = expanded.Adapters
	}

	return finalMap
}

// ClearAllAdapters Removes all adapters from the engine
func (sh *AdapterHost) ClearAllAdapters() {
	sh.adapterMapMutex.Lock()
	sh.adapterMap = make(map[string][]Adapter)
	sh.adapterMapMutex.Unlock()

	sh.addBuiltinAdapters()
}

// queryHash Calculates a hash for a given query which can be used to
// determine if two queries are identical
func queryHash(req *sdp.Query) (string, error) {
	hash := sha256.New()

	// Marshall to bytes so that we can use sha1 to compare the raw binary
	b, err := proto.Marshal(req)

	if err != nil {
		sentry.CaptureException(err)
		return "", err
	}

	return base64.URLEncoding.EncodeToString(hash.Sum(b)), nil
}

// StartPurger Starts the purger for all caching adapters
func (sh *AdapterHost) StartPurger(ctx context.Context) {
	for _, s := range sh.Adapters() {
		if c, ok := s.(CachingAdapter); ok {
			cache := c.Cache()
			if cache != nil {
				err := cache.StartPurger(ctx)
				if err != nil {
					sentry.CaptureException(fmt.Errorf("failed to start purger for adapter %s: %w", s.Name(), err))
				}
			}
		}
	}
}

func (sh *AdapterHost) Purge() {
	for _, s := range sh.Adapters() {
		if c, ok := s.(CachingAdapter); ok {
			cache := c.Cache()
			if cache != nil {
				cache.Purge(time.Now())
			}
		}
	}
}

// ClearCaches Clears caches for all caching adapters
func (sh *AdapterHost) ClearCaches() {
	for _, s := range sh.Adapters() {
		if c, ok := s.(CachingAdapter); ok {
			cache := c.Cache()
			if cache != nil {
				cache.Clear()
			}
		}
	}
}
