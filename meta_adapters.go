package discovery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

type TypeAdapter struct {
	// The SourceHost to query adapters from
	sh *AdapterHost
}

func (t *TypeAdapter) Type() string {
	return "overmind-type"
}

func (t *TypeAdapter) Name() string {
	return "overmind-type-meta-source"
}

func (t *TypeAdapter) Metadata() *sdp.AdapterMetadata {
	return &sdp.AdapterMetadata{}
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

func (t *TypeAdapter) Get(ctx context.Context, scope string, query string, ignoreCache bool) (*sdp.Item, error) {
	if !strings.Contains("global", scope) {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: fmt.Sprintf("'%v' only available in global scope", t.Type()),
		}
	}

	adapters := t.sh.AdaptersByType(query)

	if len(adapters) == 0 {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOTFOUND,
			ErrorString: fmt.Sprintf("type '%v' not found", query),
		}
	}

	return newTypeItem(adapters[0].Type()), nil
}

func (t *TypeAdapter) List(ctx context.Context, scope string, ignoreCache bool) ([]*sdp.Item, error) {
	if !strings.Contains("global", scope) {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: fmt.Sprintf("%v only available in global scope", t.Type()),
		}
	}

	typesMap := make(map[string][]Adapter)

	for _, adapter := range t.sh.Adapters() {
		if hiddenSource, ok := adapter.(HiddenAdapter); ok {
			if hiddenSource.Hidden() {
				// Skip hidden adapters on list
				continue
			}
		}
		typesMap[adapter.Type()] = append(typesMap[adapter.Type()], adapter)
	}

	items := make([]*sdp.Item, 0)

	for typ := range typesMap {
		items = append(items, newTypeItem(typ))
	}

	return items, nil
}

// Scopes Returns just global since all the Overmind types are global
func (m *TypeAdapter) Scopes() []string {
	return []string{"global"}
}

// Weight Default weight to satisfy `adapter` interface
func (m *TypeAdapter) Weight() int {
	return 100
}

// DefaultCacheDuration Defaults to a vary low value since these resources
// aren't expensive to get
func (m *TypeAdapter) DefaultCacheDuration() time.Duration {
	return time.Second
}

// Hidden These resources should be hidden
func (m *TypeAdapter) Hidden() bool {
	return true
}

type ScopeAdapter struct {
	// The AdapterHost to query adapters from
	sh *AdapterHost
}

func (t *ScopeAdapter) Type() string {
	return "overmind-scope"
}

func (t *ScopeAdapter) Name() string {
	return "overmind-scope-meta-adapter"
}

func (t *ScopeAdapter) Metadata() *sdp.AdapterMetadata {
	return &sdp.AdapterMetadata{}
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

func (t *ScopeAdapter) Get(ctx context.Context, scope string, query string, ignoreCache bool) (*sdp.Item, error) {
	if !strings.Contains("global", scope) {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: fmt.Sprintf("%v only available in global scope", t.Type()),
		}
	}

	for _, adapter := range t.sh.Adapters() {
		for _, scope := range adapter.Scopes() {
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

func (t *ScopeAdapter) List(ctx context.Context, scope string, ignoreCache bool) ([]*sdp.Item, error) {
	if !strings.Contains("global", scope) {
		return nil, &sdp.QueryError{
			ErrorType:   sdp.QueryError_NOSCOPE,
			ErrorString: fmt.Sprintf("%v only available in global scope", t.Type()),
		}
	}

	scopesMap := make(map[string]bool)

	for _, adapter := range t.sh.Adapters() {
		if hiddenAdapter, ok := adapter.(HiddenAdapter); ok {
			if hiddenAdapter.Hidden() {
				// Skip hidden adapters on list
				continue
			}
		}

		for _, scope := range adapter.Scopes() {
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
func (m *ScopeAdapter) Scopes() []string {
	return []string{"global"}
}

// Weight Default weight to satisfy `Adapter` interface
func (m *ScopeAdapter) Weight() int {
	return 100
}

// DefaultCacheDuration Defaults to a vary low value since these resources
// aren't expensive to get
func (m *ScopeAdapter) DefaultCacheDuration() time.Duration {
	return time.Second
}

// Hidden These resources should be hidden
func (m *ScopeAdapter) Hidden() bool {
	return true
}

// SourcesAdapter A source which returns the details of all running adapter as
// items
type SourcesAdapter struct {
	sh *AdapterHost

	SearchResultsLimit int
}

func (s *SourcesAdapter) Type() string {
	return "overmind-adapter"
}

func (s *SourcesAdapter) Name() string {
	return "overmind-adapter-meta-adapter"
}

func (s *SourcesAdapter) Metadata() *sdp.AdapterMetadata {
	return &sdp.AdapterMetadata{}
}

func (s *SourcesAdapter) Scopes() []string {
	return []string{"global"}
}

func (s *SourcesAdapter) Get(ctx context.Context, scope string, query string, ignoreCache bool) (*sdp.Item, error) {
	for _, adapter := range s.sh.Adapters() {
		if adapter.Name() == query {
			return s.adapterToItem(adapter)
		}
	}

	return nil, &sdp.QueryError{
		ErrorType: sdp.QueryError_NOTFOUND,
	}
}

func (s *SourcesAdapter) List(ctx context.Context, scope string, ignoreCache bool) ([]*sdp.Item, error) {
	adapters := s.sh.Adapters()
	items := make([]*sdp.Item, len(adapters))

	var item *sdp.Item
	var err error

	for i, adapter := range adapters {
		item, err = s.adapterToItem(adapter)

		if err != nil {
			return nil, sdp.NewQueryError(err)
		}

		items[i] = item
	}

	return items, nil
}

func (s *SourcesAdapter) Hidden() bool {
	return true
}

func (s *SourcesAdapter) Weight() int {
	return 100
}

func (s *SourcesAdapter) adapterToItem(adapter Adapter) (*sdp.Item, error) {
	attrMap := make(map[string]interface{})

	attrMap["name"] = adapter.Name()
	attrMap["scopes"] = adapter.Scopes()
	attrMap["weight"] = adapter.Weight()

	_, searchable := adapter.(SearchableAdapter)
	attrMap["searchable"] = searchable

	var hidden bool

	if h, ok := adapter.(HiddenAdapter); ok {
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
