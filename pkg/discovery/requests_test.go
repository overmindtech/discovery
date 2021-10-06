package discovery

import (
	"testing"

	"github.com/dylanratcliffe/sdp-go"
)

func TestExecuteRequest(t *testing.T) {
	e := Engine{
		Name: "test",
	}

	src := TestSource{
		ReturnType:     "person",
		ReturnContexts: []string{"test"},
	}

	e.AddSources(&src)

	t.Run("Basic happy-path request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Context:   "test",
			LinkDepth: 0,
		}

		items, err := e.ExecuteRequest(request)

		if err != nil {
			t.Error(err)
		}

		if x := len(src.GetCalls); x != 1 {
			t.Errorf("expected source's Get() to have been called 1 time, got %v", x)
		}

		if x := len(items); x != 1 {
			t.Errorf("expected 1 item, got %v", x)
		}
	})

	t.Run("Wrong context request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "person",
			Method:    sdp.RequestMethod_GET,
			Context:   "wrong",
			LinkDepth: 0,
		}

		_, err := e.ExecuteRequest(request)

		if ire, ok := err.(*sdp.ItemRequestError); ok {
			if ire.ErrorType != sdp.ItemRequestError_NOCONTEXT {
				t.Errorf("expected error type to be NOCONTEXT, got %v", ire.ErrorType)
			}
		} else {
			t.Errorf("expected error to be ItemRequestError, got %T", err)
		}
	})

	t.Run("Wrong type request", func(t *testing.T) {
		request := &sdp.ItemRequest{
			Type:      "house",
			Method:    sdp.RequestMethod_GET,
			Context:   "test",
			LinkDepth: 0,
		}

		_, err := e.ExecuteRequest(request)

		if ire, ok := err.(*sdp.ItemRequestError); ok {
			if ire.ErrorType != sdp.ItemRequestError_NOCONTEXT {
				t.Errorf("expected error type to be NOCONTEXT, got %v", ire.ErrorType)
			}
		} else {
			t.Errorf("expected error to be ItemRequestError, got %T", err)
		}
	})

}
