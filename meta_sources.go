package discovery

import (
	"context"
	"errors"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/simple"
	"github.com/overmindtech/sdp-go"
)

// Default number of results to retuns for a search
const DefaultSearchResultsLimit = 5

// NewMetaSource Creates a new meta source, including creation of the index
func NewMetaSource(engine *Engine) (MetaSource, error) {
	mapping := bleve.NewIndexMapping()
	mapping.DefaultAnalyzer = simple.Name

	var err error
	var ms MetaSource

	ms.Engine = engine
	ms.contextIndex, err = bleve.NewMemOnly(mapping)

	if err != nil {
		return MetaSource{}, err
	}

	ms.typeIndex, err = bleve.NewMemOnly(mapping)

	if err != nil {
		return MetaSource{}, err
	}

	return ms, err
}

type MetaSource struct {
	// The engine to query sources from
	Engine *Engine

	typeIndex    bleve.Index
	contextIndex bleve.Index

	numSourcesIndexed int // Number of sources that have been indexed
}

func searchRequest(query string) *bleve.SearchRequest {
	// Simple "starts with"
	prefix := bleve.NewPrefixQuery(query)

	// Fuzzy query, will only match longer strings but should be smarter than
	// "starts with"
	fuzzy := bleve.NewFuzzyQuery(query)
	fuzzy.Fuzziness = 2

	q := bleve.NewDisjunctionQuery(fuzzy, prefix)
	search := bleve.NewSearchRequest(q)

	return search
}

// SearchType Searches for available types based on the sources that the engine
// has, returns a list of results
func (m *MetaSource) SearchType(query string) ([]string, error) {
	return m.searchIndex(m.typeIndex, query)
}

// SearchContext Searches for available contexts based on the sources that the
// engine has, returns a list of results
func (m *MetaSource) SearchContext(query string) ([]string, error) {
	return m.searchIndex(m.contextIndex, query)
}

func (m *MetaSource) searchIndex(index bleve.Index, query string) ([]string, error) {
	if m.indexOutdated() {
		m.rebuildIndex()
	}

	searchResults, err := index.Search(searchRequest(query))

	if err != nil {
		return nil, err
	}

	results := make([]string, 0)

	for _, hit := range searchResults.Hits {
		results = append(results, hit.ID)
	}

	return results, nil
}

// indexOutdated Returns whether or not the index is outdated and needs to be rebuilt
func (m *MetaSource) indexOutdated() bool {
	var l int

	if m.Engine != nil {
		l = len(m.Engine.Sources())
	}

	return l != m.numSourcesIndexed
}

// rebuildIndex Reindexes all sources. Since sources can't be deleted we aren't
// handling that use case
func (m *MetaSource) rebuildIndex() error {
	if m.Engine == nil {
		return errors.New("no engine specified, cannot index sources")
	}

	var err error

	sources := m.Engine.Sources()

	for _, src := range sources {
		err = m.typeIndex.Index(src.Type(), src.Type())

		if err != nil {
			return err
		}

		for _, c := range src.Contexts() {
			err = m.contextIndex.Index(c, c)

			if err != nil {
				return err
			}
		}
	}

	m.numSourcesIndexed = len(sources)

	return nil
}

// SearchableData A stract that represents a source that is indexed for search
type SearchableData struct {
	Type    string `json:"type"`
	Context string `json:"context"`
}

// SourcesSource A source which returns the details of all running sources as
// items
type SourcesSource struct {
	MetaSource

	SearchResultsLimit int
}

func (s *SourcesSource) Type() string {
	return "overmind_source"
}

func (s *SourcesSource) Name() string {
	return "overmind-meta-source"
}

func (s *SourcesSource) Contexts() []string {
	return []string{
		"global",
	}
}

func (s *SourcesSource) Get(ctx context.Context, itemContext string, query string) (*sdp.Item, error) {
	if s.Engine == nil {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_OTHER,
			ErrorString: "engine not set",
		}
	}

	for _, src := range s.Engine.Sources() {
		if src.Name() == query {
			return s.sourceToItem(src)
		}
	}

	return nil, &sdp.ItemRequestError{
		ErrorType: sdp.ItemRequestError_NOTFOUND,
	}
}

func (s *SourcesSource) Find(ctx context.Context, itemContext string) ([]*sdp.Item, error) {
	if s.Engine == nil {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_OTHER,
			ErrorString: "engine not set",
		}
	}

	sources := s.Engine.Sources()
	items := make([]*sdp.Item, len(sources))

	var item *sdp.Item
	var err error

	for i, src := range sources {
		item, err = s.sourceToItem(src)

		if err != nil {
			return nil, sdp.NewItemRequestError(err)
		}

		items[i] = item
	}

	return items, nil
}

func (s *SourcesSource) Hidden() bool {
	return true
}

func (s *SourcesSource) Weight() int {
	return 100
}

func (s *SourcesSource) sourceToItem(src Source) (*sdp.Item, error) {
	attrMap := make(map[string]interface{})

	attrMap["name"] = src.Name()
	attrMap["contexts"] = src.Contexts()
	attrMap["weight"] = src.Weight()

	_, searchable := src.(SearchableSource)
	attrMap["searchable"] = searchable

	if cd, ok := src.(CacheDefiner); ok {
		attrMap["defaultCacheDuration"] = cd.DefaultCacheDuration().String()
	}

	var hidden bool

	if h, ok := src.(HiddenSource); ok {
		hidden = h.Hidden()
	} else {
		hidden = false
	}

	attrMap["hidden"] = hidden

	attributes, err := sdp.ToAttributes(attrMap)

	if err != nil {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_OTHER,
			ErrorString: err.Error(),
		}
	}

	item := sdp.Item{
		Type:            s.Type(),
		UniqueAttribute: "name",
		Context:         "global",
		Attributes:      attributes,
	}

	return &item, nil
}
