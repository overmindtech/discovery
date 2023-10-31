package discovery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

type TypeSource struct {
	// The SourceHost to query sources from
	sh *SourceHost
}

func (t *TypeSource) Type() string {
	return "overmind-type"
}

func (t *TypeSource) Name() string {
	return "overmind-type-metasource"
}

func newTypeItem(typ string) *sdp.Item {
	return &sdp.Item{
		Type:            "overmind-type",
		UniqueAttribute: "name",
		Attributes: &sdp.ItemAttributes{
			AttrStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"name": structpb.NewStringValue(typ),
				},
			},
		},
		Scope: "global",
	}
}

func (t *TypeSource) Get(ctx context.Context, scope string, query string, ignoreCache bool) (*sdp.Item, error) {
	if !strings.Contains("global", scope) {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: fmt.Sprintf("'%v' only available in global scope", t.Type()),
		}
	}

	sources := t.sh.SourcesByType(query)

	if len(sources) == 0 {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOTFOUND,
			ErrorString: fmt.Sprintf("type '%v' not found", query),
		}
	}

	return newTypeItem(sources[0].Type()), nil
}

func (t *TypeSource) List(ctx context.Context, scope string, ignoreCache bool) ([]*sdp.Item, error) {
	if !strings.Contains("global", scope) {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: fmt.Sprintf("%v only available in global scope", t.Type()),
		}
	}

	typesMap := make(map[string][]Source)

	for _, source := range t.sh.Sources() {
		if hiddenSource, ok := source.(HiddenSource); ok {
			if hiddenSource.Hidden() {
				// Skip hidden sources on list
				continue
			}
		}
		typesMap[source.Type()] = append(typesMap[source.Type()], source)
	}

	items := make([]*sdp.Item, 0)

	for typ := range typesMap {
		items = append(items, newTypeItem(typ))
	}

	return items, nil
}

// Scopes Returns just global since all the Overmind types are global
func (m *TypeSource) Scopes() []string {
	return []string{"global"}
}

// Weight Default weight to satisfy `Source` interface
func (m *TypeSource) Weight() int {
	return 100
}

// DefaultCacheDuration Defaults to a vary low value since these resources
// aren't expensive to get
func (m *TypeSource) DefaultCacheDuration() time.Duration {
	return time.Second
}

// Hidden These resources should be hidden
func (m *TypeSource) Hidden() bool {
	return true
}

type ScopeSource struct {
	// The SourceHost to query sources from
	sh *SourceHost
}

func (t *ScopeSource) Type() string {
	return "overmind-scope"
}

func (t *ScopeSource) Name() string {
	return "overmind-scope-metasource"
}

func newScopeItem(scope string) *sdp.Item {
	return &sdp.Item{
		Type:            "overmind-scope",
		UniqueAttribute: "name",
		Attributes: &sdp.ItemAttributes{
			AttrStruct: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"name": structpb.NewStringValue(scope),
				},
			},
		},
		Scope: "global",
	}
}

func (t *ScopeSource) Get(ctx context.Context, scope string, query string, ignoreCache bool) (*sdp.Item, error) {
	if !strings.Contains("global", scope) {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: fmt.Sprintf("%v only available in global scope", t.Type()),
		}
	}

	for _, source := range t.sh.Sources() {
		for _, scope := range source.Scopes() {
			if scope == query {
				return newScopeItem(scope), nil
			}
		}
	}

	return nil, &sdp.QueryError{
		ErrorType:   sdp.QueryError_NOTFOUND,
		ErrorString: fmt.Sprintf("scope '%v' not found", query),
	}
}

func (t *ScopeSource) List(ctx context.Context, scope string, ignoreCache bool) ([]*sdp.Item, error) {
	if !strings.Contains("global", scope) {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: fmt.Sprintf("%v only available in global scope", t.Type()),
		}
	}

	scopesMap := make(map[string]bool)

	for _, source := range t.sh.Sources() {
		if hiddenSource, ok := source.(HiddenSource); ok {
			if hiddenSource.Hidden() {
				// Skip hidden sources on list
				continue
			}
		}

		for _, scope := range source.Scopes() {
			scopesMap[scope] = true
		}
	}

	items := make([]*sdp.Item, 0)

	for scope := range scopesMap {
		items = append(items, newScopeItem(scope))
	}

	return items, nil
}

// Scopes Returns just global since all the Overmind types are global
func (m *ScopeSource) Scopes() []string {
	return []string{"global"}
}

// Weight Default weight to satisfy `Source` interface
func (m *ScopeSource) Weight() int {
	return 100
}

// DefaultCacheDuration Defaults to a vary low value since these resources
// aren't expensive to get
func (m *ScopeSource) DefaultCacheDuration() time.Duration {
	return time.Second
}

// Hidden These resources should be hidden
func (m *ScopeSource) Hidden() bool {
	return true
}

// SourcesSource A source which returns the details of all running sources as
// items
type SourcesSource struct {
	sh *SourceHost

	SearchResultsLimit int
}

func (s *SourcesSource) Type() string {
	return "overmind-source"
}

func (s *SourcesSource) Name() string {
	return "overmind-source-metasource"
}

func (s *SourcesSource) Scopes() []string {
	return []string{"global"}
}

func (s *SourcesSource) Get(ctx context.Context, scope string, query string, ignoreCache bool) (*sdp.Item, error) {
	for _, src := range s.sh.Sources() {
		if src.Name() == query {
			return s.sourceToItem(src)
		}
	}

	return nil, &sdp.QueryError{
		ErrorType: sdp.QueryError_NOTFOUND,
	}
}

func (s *SourcesSource) List(ctx context.Context, scope string, ignoreCache bool) ([]*sdp.Item, error) {
	sources := s.sh.Sources()
	items := make([]*sdp.Item, len(sources))

	var item *sdp.Item
	var err error

	for i, src := range sources {
		item, err = s.sourceToItem(src)

		if err != nil {
			return nil, sdp.NewQueryError(err)
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
	attrMap["scopes"] = src.Scopes()
	attrMap["weight"] = src.Weight()

	_, searchable := src.(SearchableSource)
	attrMap["searchable"] = searchable

	var hidden bool

	if h, ok := src.(HiddenSource); ok {
		hidden = h.Hidden()
	} else {
		hidden = false
	}

	attrMap["hidden"] = hidden

	attributes, err := sdp.ToAttributes(attrMap)

	if err != nil {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_OTHER,
			ErrorString: err.Error(),
		}
	}

	item := sdp.Item{
		Type:            s.Type(),
		UniqueAttribute: "name",
		Scope:           "global",
		Attributes:      attributes,
	}

	return &item, nil
}
