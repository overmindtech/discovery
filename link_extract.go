package discovery

import (
	"net"
	"net/url"
	"regexp"

	"github.com/overmindtech/sdp-go"
	"google.golang.org/protobuf/types/known/structpb"
)

// This function tries to extract linked item queries from the attributes of an
// item. It should be on items that we know are likely to contain references
// that we can discover, but are in an unstructured format which we can't
// construct the linked item queries from directly. A good example of this would
// be the env vars for a kubernetes pod, or a config map
//
// This supports extracting the following formats:
//
// - IP addresses
// - HTTP/HTTPS URLs
// - DNS names
func ExtractLinksFromAttributes(attributes *sdp.ItemAttributes) []*sdp.LinkedItemQuery {
	return extractLinksFromStructValue(attributes.GetAttrStruct())
}

func extractLinksFromValue(value *structpb.Value) []*sdp.LinkedItemQuery {
	switch value.GetKind().(type) {
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_NumberValue:
		return nil
	case *structpb.Value_StringValue:
		return extractLinksFromStringValue(value.GetStringValue())
	case *structpb.Value_BoolValue:
		return nil
	case *structpb.Value_StructValue:
		return extractLinksFromStructValue(value.GetStructValue())
	case *structpb.Value_ListValue:
		return extractLinksFromListValue(value.GetListValue())
	}

	return nil
}

func extractLinksFromStructValue(structValue *structpb.Struct) []*sdp.LinkedItemQuery {
	queries := make([]*sdp.LinkedItemQuery, 0)

	for _, value := range structValue.GetFields() {
		queries = append(queries, extractLinksFromValue(value)...)
	}

	return queries
}

func extractLinksFromListValue(list *structpb.ListValue) []*sdp.LinkedItemQuery {
	queries := make([]*sdp.LinkedItemQuery, 0)

	for _, value := range list.GetValues() {
		queries = append(queries, extractLinksFromValue(value)...)
	}

	return queries
}

// This function does all the heavy lifting for extracting linked item queries
// from strings. It will be called once for every string value in the item so
// needs to be very performant
func extractLinksFromStringValue(val string) []*sdp.LinkedItemQuery {
	if ip := net.ParseIP(val); ip != nil {
		return []*sdp.LinkedItemQuery{
			{
				Query: &sdp.Query{
					Type:   "ip",
					Method: sdp.QueryMethod_GET,
					Query:  ip.String(),
					Scope:  "global",
				},
				BlastPropagation: &sdp.BlastPropagation{
					In:  true,
					Out: true,
				},
			},
		}
	}

	if parsed, err := url.Parse(val); err == nil {
		if parsed.Scheme == "" || parsed.Host == "" {
			// If there's no scheme, we can't do anything with it
			return nil
		}

		// If it's a HTTP/HTTPS URL, we can use a HTTP query
		if parsed.Scheme == "http" || parsed.Scheme == "https" {
			return []*sdp.LinkedItemQuery{
				{
					Query: &sdp.Query{
						Type:   "http",
						Method: sdp.QueryMethod_GET,
						Query:  val,
						Scope:  "global",
					},
					BlastPropagation: &sdp.BlastPropagation{
						// If we are referencing a HTTP URL, I think it's safe
						// to assume that this is something that the current
						// resource depends on and therefore that the blast
						// radius should propagate inwards. This is a bit of a
						// guess though...
						In:  true,
						Out: false,
					},
				},
			}
		} else {
			// If it's not a HTTP/HTTPS URL, it'll be an IP or DNS name, so pass
			// back to the main function
			return extractLinksFromStringValue(parsed.Hostname())
		}
	}

	if isLikelyDNSName(val) {
		return []*sdp.LinkedItemQuery{
			{
				Query: &sdp.Query{
					Type:   "dns",
					Method: sdp.QueryMethod_SEARCH,
					Query:  val,
					Scope:  "global",
				},
				BlastPropagation: &sdp.BlastPropagation{
					In:  true,
					Out: false,
				},
			},
		}
	}

	return nil
}

// Compile a regex pattern to match the general structure of a DNS name. Limits
// each label to 1-63 characters and matches only allowed characters and ensure
// that the name has at least three sections i.e. two dots.
var dnsNameRegex = regexp.MustCompile(`^(?i)([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?\.){2,}[a-z]{2,}$`)

// This function returns true if the given string is a valid DNS name with at
// least three labels (sections)
func isLikelyDNSName(name string) bool {
	// Quick length check before the regex.
	if len(name) < 1 || len(name) > 253 {
		return false
	}

	// Check if the name matches the regex pattern.
	if !dnsNameRegex.MatchString(name) {
		return false
	}

	return true
}
