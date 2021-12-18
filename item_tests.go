// Reusable testing libraries for testing sources
package discovery

import (
	"testing"

	"github.com/overmindtech/sdp-go"
)

// TestValidateItem Checks an item to ensure it is a valid SDP item. This includes
// checking that all required attributes are populated
func TestValidateItem(t *testing.T, i *sdp.Item) {
	// Ensure that the item has the required fields set i.e.
	//
	// * Type
	// * UniqueAttribute
	// * Attributes
	if i.GetType() == "" {
		t.Errorf("Item %v has an empty Type", i.GloballyUniqueName())
	}

	if i.GetUniqueAttribute() == "" {
		t.Errorf("Item %v has an empty UniqueAttribute", i.GloballyUniqueName())
	}

	attrMap := i.GetAttributes().AttrStruct.AsMap()

	if len(attrMap) == 0 {
		t.Errorf("Attributes for item %v are empty", i.GloballyUniqueName())
	}

	// Check the attributes themselves for validity
	for k := range attrMap {
		if k == "" {
			t.Errorf("Item %v has an attribute with an empty name", i.GloballyUniqueName())
		}
	}

	// Make sure that the UniqueAttributeValue is populated
	if i.UniqueAttributeValue() == "" {
		t.Errorf("UniqueAttribute %v for item %v is empty", i.GetUniqueAttribute(), i.GloballyUniqueName())
	}

	for index, linkedItem := range i.GetLinkedItems() {
		if linkedItem.GetType() == "" {
			t.Errorf("LinkedItem %v of item %v has empty type", index, i.GloballyUniqueName())
		}

		if linkedItem.GetUniqueAttributeValue() == "" {
			t.Errorf("LinkedItem %v of item %v has empty UniqueAttributeValue", index, i.GloballyUniqueName())
		}

		// We don't need to check for an empty context here since if it's empty
		// it will just inherit the context of the parent
	}

	for index, linkedItemRequest := range i.GetLinkedItemRequests() {
		if linkedItemRequest.GetType() == "" {
			t.Errorf("LinkedItemRequest %v of item %v has empty type", index, i.GloballyUniqueName())
		}

		if linkedItemRequest.GetMethod() != sdp.RequestMethod_FIND {
			if linkedItemRequest.GetQuery() == "" {
				t.Errorf("LinkedItemRequest %v of item %v has empty query. This is not allowed unless the method is FIND", index, i.GloballyUniqueName())
			}
		}

		if linkedItemRequest.GetContext() == "" {
			t.Errorf("LinkedItemRequest %v of item %v has empty context", index, i.GloballyUniqueName())
		}
	}
}

// TestValidateItems Runs TestValidateItem on many items
func TestValidateItems(t *testing.T, is []*sdp.Item) {
	for _, i := range is {
		TestValidateItem(t, i)
	}
}
