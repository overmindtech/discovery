package discovery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/token/camelcase"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/letter"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

// Default number of results to retuns for a search
const DefaultSearchResultsLimit = 5

// NewMetaSource Creates a new meta source, including creation of the index
func NewMetaSource(engine *Engine) (MetaSource, error) {
	var err error

	mapping := bleve.NewIndexMapping()

	err = mapping.AddCustomAnalyzer("custom", map[string]interface{}{
		"type":         custom.Name,
		"char_filters": []string{},
		"tokenizer":    letter.Name,
		"token_filters": []string{
			camelcase.Name,
			lowercase.Name,
		},
	})

	if err != nil {
		return MetaSource{}, err
	}

	mapping.DefaultAnalyzer = "custom"

	var ms MetaSource

	ms.engine = engine
	ms.indexedSources = make(map[string]Source)
	ms.index, err = bleve.NewMemOnly(mapping)

	if err != nil {
		return MetaSource{}, err
	}

	return ms, err
}

func searchRequest(query string, field Field) *bleve.SearchRequest {
	// Simple "starts with"
	prefix := bleve.NewPrefixQuery(query)

	// Fuzzy query, will only match longer strings but should be smarter than
	// "starts with"
	fuzzy := bleve.NewFuzzyQuery(query)

	// Tokenize the input and search for those too
	match := bleve.NewMatchQuery(query)

	q := bleve.NewDisjunctionQuery(fuzzy, prefix, match)
	search := bleve.NewSearchRequest(q)
	search.IncludeLocations = true
	search.Fields = []string{string(field)}

	return search
}

type Field string

const (
	Type     Field = "Type"
	Contexts Field = "Contexts"
)

type SearchResult struct {
	Value          string
	RelatedSources []Source
}

// interimResult Uses a mapt to ensure uniqueness, is eventually translated to
// SearchResult
type interimResult struct {
	Value          string
	RelatedSources map[string]Source // Map to ensure uniqueness
	Score          float64
}

type interimResults map[string]interimResult

// ToResults Converts interim results to final search results by removing maps
// and sorting
func (i interimResults) ToResults() []SearchResult {
	finalResults := make([]SearchResult, 0)

	// Convert to a slice for sorting
	interimSlice := make([]interimResult, len(i))
	var index int

	for _, result := range i {
		interimSlice[index] = result
		index++
	}

	// Sort
	sort.Slice(interimSlice, func(i, j int) bool {
		// Sort in reverse, highest score first
		return interimSlice[i].Score > interimSlice[j].Score
	})

	for _, res := range interimSlice {
		sources := make([]Source, 0)

		for _, source := range res.RelatedSources {
			sources = append(sources, source)
		}

		finalResults = append(finalResults, SearchResult{
			Value:          res.Value,
			RelatedSources: sources,
		})
	}

	return finalResults
}

type MetaSource struct {
	// The engine to query sources from
	engine *Engine

	// The actual Bleve index
	index bleve.Index

	// A map of all sources that have been indexed by their index ID
	indexedSources map[string]Source

	numSourcesIndexed int // Number of sources that have been indexed
}

// Contexts Returns just global since all the Overmind types are global
func (m *MetaSource) Contexts() []string {
	return []string{"global"}
}

// Weight Default weight to satidy `Source` interface
func (m *MetaSource) Weight() int {
	return 100
}

// DefaultCacheDuration Defaults to a vary low value since these resources
// aren't expensive to get
func (m *MetaSource) DefaultCacheDuration() time.Duration {
	return time.Second
}

// Hidden These resources shsould be hidden
func (m *MetaSource) Hidden() bool {
	return true
}

func (m *MetaSource) All(field Field) []SearchResult {
	if m.indexOutdated() {
		m.rebuildIndex()
	}

	interim := make(interimResults)

	for _, src := range m.indexedSources {
		switch field {
		case Type:
			mergeInterminResult(interim, src.Type(), src, 0)
		case Contexts:
			for _, context := range src.Contexts() {
				mergeInterminResult(interim, context, src, 0)
			}
		}
	}

	return interim.ToResults()
}

// SearchField Searaches the sources index by a particlar field (Type or
// Context) and returns a list of results. Each result contains the value and a
// list of sources that this is related to
func (m *MetaSource) SearchField(field Field, query string) ([]SearchResult, error) {
	if m.indexOutdated() {
		m.rebuildIndex()
	}

	searchResults, err := m.index.Search(searchRequest(query, field))

	if err != nil {
		return nil, err
	}

	// Map of the actual found value to the full result
	results := make(interimResults)

	// I'm not proud of this. But the data format that is returned from Bleve is
	// pretty complex and this is the best way I could think of to get the data
	// out. Stepping through this with a debuggers is recommended
	for _, hit := range searchResults.Hits {
		// Extract which thing (type or context) that the result is relevant to
		for fieldName, locationMap := range hit.Locations {
			if fieldName != string(field) {
				// Ignore fields other than the one we care about
				continue
			}

			for _, locations := range locationMap {
				for _, location := range locations {
					if len(location.ArrayPositions) == 0 {
						// This means it wasn't an array
						value := hit.Fields[fieldName].(string)
						relatedSource := m.indexedSources[hit.ID]

						mergeInterminResult(results, value, relatedSource, hit.Score)
					} else {
						for _, position := range location.ArrayPositions {
							if relevantSlice, ok := hit.Fields[fieldName].([]interface{}); ok {
								value := relevantSlice[position].(string)
								relatedSource := m.indexedSources[hit.ID]

								mergeInterminResult(results, value, relatedSource, hit.Score)
							}
						}
					}
				}
			}
		}
	}

	return results.ToResults(), nil
}

// rebuildIndex Reindexes all sources. Since sources can't be deleted we aren't
// handling that use case
func (m *MetaSource) rebuildIndex() error {
	if m.engine == nil {
		return errors.New("no engine specified, cannot index sources")
	}

	var err error

	sources := m.engine.Sources()

	for _, src := range sources {
		err = m.index.Index(src.Name(), SourceDetails{
			Type:     src.Type(),
			Name:     src.Name(),
			Contexts: src.Contexts(),
		})

		if err != nil {
			return err
		}

		m.indexedSources[src.Name()] = src
	}

	m.numSourcesIndexed = len(sources)

	return nil
}

// mergeInterminResult Merges a result into an existng map, avoiding duplication
func mergeInterminResult(results interimResults, value string, relatedSource Source, score float64) {
	if result, ok := results[value]; ok {
		// Merge source into existing
		result.RelatedSources[relatedSource.Name()] = relatedSource
	} else {
		// Create new
		results[value] = interimResult{
			Value: value,
			Score: score,
			RelatedSources: map[string]Source{
				relatedSource.Name(): relatedSource,
			},
		}
	}
}

// indexOutdated Returns whether or not the index is outdated and needs to be rebuilt
func (m *MetaSource) indexOutdated() bool {
	var l int

	if m.engine != nil {
		l = len(m.engine.Sources())
	}

	return l != m.numSourcesIndexed
}

type SourceDetails struct {
	Type     string
	Contexts []string
	Name     string
}

type TypeSource struct {
	MetaSource
}

func NewTypeSource(engine *Engine) (*TypeSource, error) {
	ms, err := NewMetaSource(engine)

	if err != nil {
		return nil, err
	}

	return &TypeSource{
		MetaSource: ms,
	}, nil
}

func (t *TypeSource) Type() string {
	return "overmind-type"
}

func (t *TypeSource) Name() string {
	return "overmind-type-metasource"
}

func resultToItem(result SearchResult, itemType string) *sdp.Item {
	item := sdp.Item{
		Type:            itemType,
		UniqueAttribute: "name",
		Context:         "global",
		Attributes: &sdp.ItemAttributes{
			AttrStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"name": structpb.NewStringValue(result.Value),
				},
			},
		},
	}

	for _, src := range result.RelatedSources {
		item.LinkedItemRequests = append(item.LinkedItemRequests, &sdp.ItemRequest{
			Type:   "overmind-source",
			Method: sdp.RequestMethod_GET,
			Query:  src.Name(),
		})
	}

	return &item
}

func (t *TypeSource) Get(ctx context.Context, itemContext string, query string) (*sdp.Item, error) {
	if itemContext != "global" {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: "overmind-type only available in global context",
		}
	}

	results, err := t.SearchField(Type, query)

	if err != nil {
		return nil, sdp.NewItemRequestError(err)
	}

	if len(results) == 0 || results[0].Value != query {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOTFOUND,
			ErrorString: fmt.Sprintf("type %v not found", query),
		}
	}

	return resultToItem(results[0], t.Type()), nil
}

func (t *TypeSource) Find(ctx context.Context, itemContext string) ([]*sdp.Item, error) {
	if itemContext != "global" {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: "overmind-type only available in global context",
		}
	}

	results := t.All(Type)
	items := make([]*sdp.Item, len(results))
	var i int

	for _, res := range results {
		items[i] = resultToItem(res, t.Type())
		i++
	}

	return items, nil
}

// Search Searches for a type by name, this accepts any search string and is
// intended to be used as an autocomplete service, where a user starts typing
// and we exectute a search with what they have typed so far.
func (t *TypeSource) Search(ctx context.Context, itemContext string, query string) ([]*sdp.Item, error) {
	if itemContext != "global" {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: "overmind-type only available in global context",
		}
	}

	results, err := t.SearchField(Type, query)

	if err != nil {
		return nil, sdp.NewItemRequestError(err)
	}

	items := make([]*sdp.Item, len(results))
	var i int

	for _, res := range results {
		items[i] = resultToItem(res, t.Type())
		i++
	}

	return items, nil
}

// TODO: Redo everything below this

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
	if s.engine == nil {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_OTHER,
			ErrorString: "engine not set",
		}
	}

	for _, src := range s.engine.Sources() {
		if src.Name() == query {
			return s.sourceToItem(src)
		}
	}

	return nil, &sdp.ItemRequestError{
		ErrorType: sdp.ItemRequestError_NOTFOUND,
	}
}

func (s *SourcesSource) Find(ctx context.Context, itemContext string) ([]*sdp.Item, error) {
	if s.engine == nil {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_OTHER,
			ErrorString: "engine not set",
		}
	}

	sources := s.engine.Sources()
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
