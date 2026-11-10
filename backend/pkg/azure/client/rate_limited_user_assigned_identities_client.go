// Copyright 2026 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"sync"

	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/lru"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi"
)

const (
	// userAssignedIdentitiesRateLimiterQPS is the sustained rate, in queries per second, allowed for User Assigned Identities API Get calls made as any single Service Managed Identity
	// principal. ARM enforces a per-principal read budget of a 250-token bucket refilled at 25 tokens per second; in INT every cluster's SMI resolves to the same mock principal, and that
	// shared principal was measured by Azure support (case 2610230050003267, incident AROSLSRE-2530) at an average of 38.62 GETs/s, peaking at 346/s, drawing 3750 HTTP 429 responses in
	// an 11-minute window. QPS is deliberately set below the 25 tokens/s refill (rather than exactly at it) to leave headroom under the budget: together with the burst below, the token
	// bucket's worst-case admission over any 10-second span is userAssignedIdentitiesRateLimiterBurst + QPS*10 = 25 + 225 = 250 tokens, exactly the bucket's capacity, instead of
	// overshooting it.
	userAssignedIdentitiesRateLimiterQPS = 22.5

	// userAssignedIdentitiesRateLimiterBurst is the maximum number of User Assigned Identities API Get calls allowed back-to-back, as a single Service Managed Identity principal, before
	// throttling starts. Kept at 25 (10% of ARM's 250-token per-principal read bucket) so the token bucket's worst-case admission over any 10-second span stays within the documented
	// bucket capacity; see userAssignedIdentitiesRateLimiterQPS for the full calculation.
	userAssignedIdentitiesRateLimiterBurst = 25

	// userAssignedIdentitiesRateLimitersCacheSize bounds how many distinct Service Managed Identity principals' rate limiters a single builder caches at once. 10000 is comfortably
	// enough to cache every distinct cluster Service Managed Identity expected to be handled by a single backend replica, while still capping worst-case memory growth instead of
	// letting the cache grow for the lifetime of the process. Once the limit is exceeded, building a client for a new principal evicts the least-recently-used principal's rate limiter.
	userAssignedIdentitiesRateLimitersCacheSize = 10000
)

// rateLimitedUserAssignedIdentitiesClient wraps a UserAssignedIdentitiesClient so that every Get first waits for a token from a shared flowcontrol.RateLimiter. This throttles our
// outgoing read rate to stay under ARM's per-principal read budget, instead of reacting to HTTP 429 responses after the fact.
//
// Known residual case, deliberately out of scope: this limiter only paces the UserAssignedIdentities API Get calls issued through ServiceManagedIdentityClientBuilder. In INT and dev
// every cluster's SMI resolves to the same mock principal, whose ARM read budget is also consumed by other components' reads made as that principal (for example the clusters-service
// NIC/VM/LoadBalancer reads tracked in https://issues.redhat.com/browse/ARO-30467 and https://issues.redhat.com/browse/ARO-30468). This limiter neither sees nor paces that sibling
// traffic: if it alone exceeds the budget, ARM keeps throttling the shared principal and these paced reads can still draw HTTP 429 responses, producing a bounded (at
// userAssignedIdentitiesRateLimiterQPS, without the self-amplification this wrapper removes) but non-zero stream of sync errors. Those still-failing passes also keep writing
// ServiceProviderCluster status (a new etag per varying error text), which immediately re-enqueues every controller watching ServiceProviderClusters — including
// ClusterClusterServiceCreate — so alerting actuated on that downstream churn also persists, paced by this token bucket rather than firing in a tight loop. In production each
// cluster's SMI is its own principal, so this contention does not arise.
type rateLimitedUserAssignedIdentitiesClient struct {
	inner       UserAssignedIdentitiesClient
	rateLimiter flowcontrol.RateLimiter
}

var _ UserAssignedIdentitiesClient = (*rateLimitedUserAssignedIdentitiesClient)(nil)

// Get waits for a token from the shared rate limiter (returning its error, e.g. from context cancellation, without calling the API if one isn't available) and then delegates to the
// inner client. Reads are rate limited because they are the traffic that exhausted ARM's per-principal read budget.
func (c *rateLimitedUserAssignedIdentitiesClient) Get(ctx context.Context, resourceGroupName, resourceName string, options *armmsi.UserAssignedIdentitiesClientGetOptions) (armmsi.UserAssignedIdentitiesClientGetResponse, error) {
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return armmsi.UserAssignedIdentitiesClientGetResponse{}, err
	}
	return c.inner.Get(ctx, resourceGroupName, resourceName, options)
}

// CreateOrUpdate is a write operation, not the read traffic that exhausted ARM's per-principal read budget, so it delegates directly to the inner client.
func (c *rateLimitedUserAssignedIdentitiesClient) CreateOrUpdate(ctx context.Context, resourceGroupName, resourceName string, parameters armmsi.Identity, options *armmsi.UserAssignedIdentitiesClientCreateOrUpdateOptions) (armmsi.UserAssignedIdentitiesClientCreateOrUpdateResponse, error) {
	return c.inner.CreateOrUpdate(ctx, resourceGroupName, resourceName, parameters, options)
}

// Delete is a write operation, not the read traffic that exhausted ARM's per-principal read budget, so it delegates directly to the inner client.
func (c *rateLimitedUserAssignedIdentitiesClient) Delete(ctx context.Context, resourceGroupName, resourceName string, options *armmsi.UserAssignedIdentitiesClientDeleteOptions) (armmsi.UserAssignedIdentitiesClientDeleteResponse, error) {
	return c.inner.Delete(ctx, resourceGroupName, resourceName, options)
}

// userAssignedIdentitiesRateLimiters hands out one flowcontrol.RateLimiter per Service Managed Identity principal, keyed by the client ID of the credential that principal's clients
// authenticate as, so that every UserAssignedIdentitiesClient built for the same principal shares that principal's ARM read budget. ARM's read limits apply per calling principal, not to
// the backend as a whole, so principals must not share a single rate limiter. Per-principal rate limiters are kept in a size-bounded, least-recently-used cache (see
// userAssignedIdentitiesRateLimitersCacheSize) so memory use stays bounded regardless of how many distinct principals are seen over the process's lifetime: once the cache's capacity is
// reached, caching a new principal evicts the least-recently-used principal's rate limiter to make room.
type userAssignedIdentitiesRateLimiters struct {
	newRateLimiter func() flowcontrol.RateLimiter
	// rateLimiters caches each principal's own flowcontrol.RateLimiter, keyed by principal client ID (string) and storing a flowcontrol.RateLimiter as the value.
	rateLimiters *lru.Cache
	// rateLimitersMu guards access to rateLimiters, ensuring that only one goroutine can add or evict a principal's rate limiter at a time.
	rateLimitersMu sync.Mutex
}

// newUserAssignedIdentitiesRateLimiters returns userAssignedIdentitiesRateLimiters that create each principal's rate limiter with
// flowcontrol.NewTokenBucketRateLimiter(userAssignedIdentitiesRateLimiterQPS, userAssignedIdentitiesRateLimiterBurst).
func newUserAssignedIdentitiesRateLimiters() *userAssignedIdentitiesRateLimiters {
	return &userAssignedIdentitiesRateLimiters{
		newRateLimiter: func() flowcontrol.RateLimiter {
			return flowcontrol.NewTokenBucketRateLimiter(userAssignedIdentitiesRateLimiterQPS, userAssignedIdentitiesRateLimiterBurst)
		},
		rateLimiters: lru.New(userAssignedIdentitiesRateLimitersCacheSize),
	}
}

// getOrCreate returns the rate limiter cached for principalClientID, marking it as the most-recently-used entry. If principalClientID isn't cached yet, getOrCreate creates its rate
// limiter via r.newRateLimiter and caches it, letting the underlying LRU cache evict the least-recently-used principal's rate limiter if it's already at capacity.
func (r *userAssignedIdentitiesRateLimiters) getOrCreate(principalClientID string) flowcontrol.RateLimiter {
	if rateLimiter, ok := r.rateLimiters.Get(principalClientID); ok {
		return rateLimiter.(flowcontrol.RateLimiter)
	}

	r.rateLimitersMu.Lock()
	defer r.rateLimitersMu.Unlock()

	// Re-check now that we hold the lock: another goroutine may have created and cached principalClientID's rate limiter between our unlocked Get above and acquiring the lock.
	if rateLimiter, ok := r.rateLimiters.Get(principalClientID); ok {
		return rateLimiter.(flowcontrol.RateLimiter)
	}

	rateLimiter := r.newRateLimiter()
	r.rateLimiters.Add(principalClientID, rateLimiter)
	return rateLimiter
}
