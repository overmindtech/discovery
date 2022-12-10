package discovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/token/camelcase"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/letter"
	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

// Default number of results to returns for a search
const DefaultSearchResultsLimit = 5

// NewMetaSource Creates a new meta source, including creation of the index
func NewMetaSource(engine *Engine, mode Field) (*MetaSource, error) {
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
		return nil, err
	}

	mapping.DefaultAnalyzer = "custom"

	var itemType string

	switch mode {
	case Type:
		itemType = "overmind-type"
	case Context:
		itemType = "overmind-context"
	}

	var ms MetaSource

	ms.engine = engine
	ms.itemType = itemType
	ms.field = mode
	ms.contextMap = make(map[string][]Source)
	ms.typeMap = make(map[string][]Source)
	ms.contextIndex, err = bleve.NewMemOnly(mapping)

	if err != nil {
		return nil, err
	}

	ms.typeIndex, err = bleve.NewMemOnly(mapping)

	if err != nil {
		return nil, err
	}

	return &ms, err
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

	return search
}

type Field string

const (
	// TODO: remember the itoa thing
	Type    Field = "Type"
	Context Field = "Context"
)

type SearchResult struct {
	Value          string
	RelatedSources []Source
}

type MetaSource struct {
	field    Field  // The field that we should search
	itemType string // The name of the types of items to returns

	// The engine to query sources from
	engine *Engine

	// The actual Bleve indexes
	contextIndex bleve.Index
	typeIndex    bleve.Index

	// Maps used for remembering which source a type or context was related to
	contextMap map[string][]Source
	typeMap    map[string][]Source

	numSourcesIndexed int // Number of sources that have been indexed
}

func (t *MetaSource) Type() string {
	return t.itemType
}

func (t *MetaSource) Name() string {
	return fmt.Sprintf("%v-metasource", t.itemType)
}

func (t *MetaSource) Get(ctx context.Context, itemContext string, query string) (*sdp.Item, error) {
	if itemContext != "global" {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: fmt.Sprintf("%v only available in global context", t.Type()),
		}
	}

	results, err := t.SearchField(t.field, query)

	if err != nil {
		return nil, sdp.NewItemRequestError(err)
	}

	if len(results) == 0 || results[0].Value != query {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOTFOUND,
			ErrorString: fmt.Sprintf("context %v not found", query),
		}
	}

	return resultToItem(results[0], t.Type()), nil
}

func (t *MetaSource) Find(ctx context.Context, itemContext string) ([]*sdp.Item, error) {
	if itemContext != "global" {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: fmt.Sprintf("%v only available in global context", t.Type()),
		}
	}

	results := t.All(t.field)
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
// and we execute a search with what they have typed so far.
func (t *MetaSource) Search(ctx context.Context, itemContext string, query string) ([]*sdp.Item, error) {
	if itemContext != "global" {
		return nil, &sdp.ItemRequestError{
			ErrorType:   sdp.ItemRequestError_NOCONTEXT,
			ErrorString: fmt.Sprintf("%v only available in global context", t.Type()),
		}
	}

	results, err := t.SearchField(t.field, query)

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

// Contexts Returns just global since all the Overmind types are global
func (m *MetaSource) Contexts() []string {
	return []string{"global"}
}

// Weight Default weight to satisfy `Source` interface
func (m *MetaSource) Weight() int {
	return 100
}

// DefaultCacheDuration Defaults to a vary low value since these resources
// aren't expensive to get
func (m *MetaSource) DefaultCacheDuration() time.Duration {
	return time.Second
}

// Hidden These resources should be hidden
func (m *MetaSource) Hidden() bool {
	return true
}

// All Returns all results for that field
func (m *MetaSource) All(field Field) []SearchResult {
	if m.indexOutdated() {
		m.rebuildIndex()
	}

	results := make([]SearchResult, 0)
	var relevantMap map[string][]Source

	switch field {
	case Type:
		relevantMap = m.typeMap
	case Context:
		relevantMap = m.contextMap
	}

	for name, sources := range relevantMap {
		results = append(results, SearchResult{
			Value:          name,
			RelatedSources: sources,
		})
	}

	return results
}

// SearchField Searches the sources index by a particular field (Type or
// Context) and returns a list of results. Each result contains the value and a
// list of sources that this is related to
func (m *MetaSource) SearchField(field Field, query string) ([]SearchResult, error) {
	if m.indexOutdated() {
		m.rebuildIndex()
	}

	var index bleve.Index

	switch field {
	case Type:
		index = m.typeIndex
	case Context:
		index = m.contextIndex
	default:
		return nil, errors.New("unsupported field")
	}

	searchResults, err := index.Search(searchRequest(query, field))

	if err != nil {
		return nil, err
	}

	// Map of the actual found value to the full result
	results := make([]SearchResult, 0)

	var sourceMap map[string][]Source

	// I'm not proud of this. But the data format that is returned from Bleve is
	// pretty complex and this is the best way I could think of to get the data
	// out. Stepping through this with a debuggers is recommended
	for _, hit := range searchResults.Hits {
		switch field {
		case Type:
			sourceMap = m.typeMap
		case Context:
			sourceMap = m.contextMap
		}

		results = append(results, SearchResult{
			Value:          hit.ID,
			RelatedSources: sourceMap[hit.ID],
		})
	}

	return results, nil
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
		err = m.indexField(Type, src.Type(), src)

		if err != nil {
			return err
		}

		for _, contextName := range src.Contexts() {
			err = m.indexField(Context, contextName, src)

			if err != nil {
				return err
			}
		}
	}

	m.numSourcesIndexed = len(sources)

	return nil
}

// indexField Indexes a given field. The user should pass the field you want ot
// index, its value, and the source that it is related to
func (m *MetaSource) indexField(field Field, value string, src Source) error {
	var index bleve.Index
	var srcMap map[string][]Source
	var err error

	switch field {
	case Type:
		index = m.typeIndex
		srcMap = m.typeMap
	case Context:
		index = m.contextIndex
		srcMap = m.contextMap
	default:
		return fmt.Errorf("type %v cannot be indexed", field)
	}

	err = index.Index(value, value)

	if err != nil {
		return err
	}

	if _, exists := srcMap[value]; !exists {
		srcMap[value] = make([]Source, 0)
	}

	srcMap[value] = append(srcMap[value], src)

	return nil
}

// indexOutdated Returns whether or not the index is outdated and needs to be rebuilt
func (m *MetaSource) indexOutdated() bool {
	var l int

	if m.engine != nil {
		l = len(m.engine.Sources())
	}

	return l != m.numSourcesIndexed
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

// SourcesSource A source which returns the details of all running sources as
// items
type SourcesSource struct {
	MetaSource

	SearchResultsLimit int
}

func (s *SourcesSource) Type() string {
	return "overmind-source"
}

func (s *SourcesSource) Name() string {
	return "overmind-source-metasource"
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
