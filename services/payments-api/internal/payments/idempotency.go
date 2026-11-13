package payments

import "strings"

// RegionScopedKey namespaces a client-supplied idempotency key with the region that owns
// the payment.
//
// The client contract does not change: callers keep sending the same reference. What
// changes is what is stored. Without the namespace, the same raw key used independently in
// region A and region B would map to a single payments row. Two customers using
// "order-1" in different regions are different payments, and a replicated event must never
// be mistaken for a local one. The region prefix makes the stored key unique per owner.
func RegionScopedKey(region, key string) string {
	if region == "" {
		region = "A"
	}
	return region + ":" + key
}

// EnsureRegionScoped prefixes key with region unless it already carries that exact prefix.
// It is used on the replication path, where a locally created event arrives already scoped
// but an event produced before namespacing may arrive raw.
func EnsureRegionScoped(region, key string) string {
	if region == "" {
		region = "A"
	}
	prefix := region + ":"
	if strings.HasPrefix(key, prefix) {
		return key
	}
	return prefix + key
}

// StripRegionPrefix returns the client-facing key from a region-scoped key. Reporting and
// audit views use it so an operator sees the reference the customer used, not the internal
// namespace.
func StripRegionPrefix(key string) string {
	if i := strings.IndexByte(key, ':'); i >= 0 {
		return key[i+1:]
	}
	return key
}
