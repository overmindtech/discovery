package discovery

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/overmindtech/sdp-go"
	sdpv2 "github.com/overmindtech/sdp-go/sdp/v2"
	"github.com/overmindtech/sdp-go/sdp/v2/sdpconnect"
)

type SdpV2Adapter struct {
	Source Source
}

// assert interface
var _ sdpconnect.SourceServiceHandler = (*SdpV2Adapter)(nil)

// Query implements sdpconnect.SourceServiceHandler.
func (a *SdpV2Adapter) Query(ctx context.Context, req *connect.Request[sdpv2.QueryRequest], res *connect.ServerStream[sdpv2.QueryResponse]) error {
	q := req.Msg.GetQuery()
	if q == nil {
		return res.Send(&sdpv2.QueryResponse{
			Response: &sdpv2.QueryResponse_Error{
				Error: &sdpv2.QueryError{
					ErrorType:   sdpv2.QueryError_ERROR_TYPE_PERMANENT_ERROR,
					ErrorString: "no query provided",
				},
			},
		})
	}
	switch q.Method {
	case sdpv2.QueryMethod_QUERY_METHOD_GET:
		item, err := a.Source.Get(ctx, q.Scope, q.Query, q.IgnoreCache)
		if err != nil {
			return handleQueryError(err, res)
		}
		if item != nil {
			err = sendConvertedItem(item, res)
			if err != nil {
				return err
			}
		}
	case sdpv2.QueryMethod_QUERY_METHOD_LIST:
		items, err := a.Source.List(ctx, q.Scope, q.IgnoreCache)
		if err != nil {
			return handleQueryError(err, res)
		}
		for _, item := range items {
			err = sendConvertedItem(item, res)
			if err != nil {
				return err
			}
		}
	case sdpv2.QueryMethod_QUERY_METHOD_SEARCH:
		searchableSource, ok := a.Source.(SearchableSource)
		if !ok {
			return res.Send(&sdpv2.QueryResponse{
				Response: &sdpv2.QueryResponse_Error{
					Error: &sdpv2.QueryError{
						ErrorType:   sdpv2.QueryError_ERROR_TYPE_PERMANENT_ERROR,
						ErrorString: "source does not support search",
					},
				},
			})
		}
		items, err := searchableSource.Search(ctx, q.Scope, q.Query, q.IgnoreCache)
		if err != nil {
			return handleQueryError(err, res)
		}
		for _, item := range items {
			err = sendConvertedItem(item, res)
			if err != nil {
				return err
			}
		}
	}
	return errors.New("not implemented")
}

func sendConvertedItem(item *sdp.Item, res *connect.ServerStream[sdpv2.QueryResponse]) error {
	result, edges := item.ToV2()
	err := res.Send(&sdpv2.QueryResponse{
		Response: &sdpv2.QueryResponse_Item{
			Item: result,
		},
	})
	if err != nil {
		return err
	}
	for _, edge := range edges {
		err := res.Send(&sdpv2.QueryResponse{
			Response: &sdpv2.QueryResponse_Edge{
				Edge: edge,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// handleQueryError handles potential errors returned by the Source, either passing on QueryErrors or wrapping other errors in a QueryError
func handleQueryError(err error, res *connect.ServerStream[sdpv2.QueryResponse]) error {
	var qe *sdpv2.QueryError
	if errors.As(err, &qe) {
		return res.Send(&sdpv2.QueryResponse{
			Response: &sdpv2.QueryResponse_Error{
				Error: qe,
			},
		})
	}

	return res.Send(&sdpv2.QueryResponse{
		Response: &sdpv2.QueryResponse_Error{
			Error: &sdpv2.QueryError{
				ErrorType:   sdpv2.QueryError_ERROR_TYPE_IMPLEMENTATION_ERROR,
				ErrorString: err.Error(),
			},
		},
	})
}
