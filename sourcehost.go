package discovery

import (
	"crypto/sha1"
	"encoding/base64"
	"sort"
	"sync"

	"github.com/getsentry/sentry-go"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/proto"
)

// SourceHost This struct holds references to all Sources in a process
// and provides utility functions to work with them. Methods of this
// struct are safe to call concurrently.
type SourceHost struct {
	// Map of types to all sources for that type
	sourceMap      map[string][]Source
	sourceMapMutex sync.RWMutex
}

func NewSourceHost() (*SourceHost, error) {
	sh := &SourceHost{
		sourceMap: make(map[string][]Source),
	}

	// Add meta-sources so that we can respond to queries for `overmind-type`,
	// `overmind-scope` and `overmind-source` resources
	sh.AddSources(&TypeSource{sh: sh})
	sh.AddSources(&ScopeSource{sh: sh})
	sh.AddSources(&SourcesSource{sh: sh})

	return sh, nil
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

// ExpandQuery Expands queries with wildcards to no longer contain wildcards.
// Meaning that if we support 5 types, and a query comes in with a wildcard
// type, this function will expand that query into 5 queries, one for each
// type.
//
// The same goes for scopes, if we have a query with a wildcard scope, and
// a single source that supports 5 scopes, we will end up with 5 queries. The
// exception to this is if we have a source that supports all scopes, but is
// unable to list them. In this case there will still be some queries with
// wildcard scopes as they can't be expanded
//
// This functions returns a map of queries with the sources that they should be
// run against
func (sh *SourceHost) ExpandQuery(q *sdp.Query) map[*sdp.Query][]Source {
	queries := make(map[string]*struct {
		Query   *sdp.Query
		Sources []Source
	})

	var checkSources []Source

	if IsWildcard(q.Type) {
		// If the query has a wildcard type, all non-hidden sources might try
		// to respond
		checkSources = sh.VisibleSources()
	} else {
		// If the type is specific, pull just sources for that type
		checkSources = sh.SourcesByType(q.Type)
	}

	for _, src := range checkSources {
		// Calculate if the source is hidden
		isHidden := false
		if hs, ok := src.(HiddenSource); ok {
			isHidden = hs.Hidden()
		}

		for _, sourceScope := range src.Scopes() {
			// Create a new query if:
			//
			// * The source supports all scopes, or
			// * The query scope is a wildcard (and the source is not hidden), or
			// * The query scope matches source scope
			if IsWildcard(sourceScope) || (IsWildcard(q.Scope) && !isHidden) || sourceScope == q.Scope {
				var scope string

				// Choose the more specific scope
				if IsWildcard(sourceScope) {
					scope = q.Scope
				} else {
					scope = sourceScope
				}

				q := sdp.Query{
					Type:            src.Type(),
					Method:          q.Method,
					Query:           q.Query,
					Scope:           scope,
					ItemSubject:     q.ItemSubject,
					ResponseSubject: q.ResponseSubject,
					LinkDepth:       q.LinkDepth,
					IgnoreCache:     q.IgnoreCache,
					UUID:            q.UUID,
					Timeout:         q.Timeout,
				}

				// deal with duplicate queries after expansion
				hash, err := queryHash(&q)

				if err == nil {
					if existing, ok := queries[hash]; ok {
						existing.Sources = append(existing.Sources, src)
					} else {
						queries[hash] = &struct {
							Query   *sdp.Query
							Sources []Source
						}{
							Query: &q,
							Sources: []Source{
								src,
							},
						}
					}
				}
			}
		}
	}

	// Convert back to final map
	finalMap := make(map[*sdp.Query][]Source)
	for _, expanded := range queries {
		finalMap[expanded.Query] = expanded.Sources
	}

	return finalMap
}

// queryHash Calculates a hash for a given query which can be used to
// determine if two queries are identical
func queryHash(req *sdp.Query) (string, error) {
	hash := sha1.New()

	// Marshall to bytes so that we can use sha1 to compare the raw binary
	b, err := proto.Marshal(req)

	if err != nil {
		sentry.CaptureException(err)
		return "", err
	}

	return base64.URLEncoding.EncodeToString(hash.Sum(b)), nil
}
