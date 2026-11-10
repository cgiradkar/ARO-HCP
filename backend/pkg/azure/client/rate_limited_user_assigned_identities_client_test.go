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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/lru"
	"k8s.io/utils/ptr"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi"
)

func TestRateLimitedUserAssignedIdentitiesClient_Get(t *testing.T) {
	t.Run("delegates to the inner client once the rate limiter admits the request", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockInner := NewMockUserAssignedIdentitiesClient(ctrl)
		wantResponse := armmsi.UserAssignedIdentitiesClientGetResponse{
			Identity: armmsi.Identity{Name: ptr.To("identity")},
		}
		mockInner.EXPECT().Get(gomock.Any(), "rg", "identity", nil).Return(wantResponse, nil).Times(1)

		client := &rateLimitedUserAssignedIdentitiesClient{inner: mockInner, rateLimiter: flowcontrol.NewFakeAlwaysRateLimiter()}

		result, err := client.Get(context.Background(), "rg", "identity", nil)
		require.NoError(t, err)
		assert.Equal(t, wantResponse, result)
	})

	t.Run("returns the rate limiter's error without calling the inner client when throttled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockInner := NewMockUserAssignedIdentitiesClient(ctrl)
		mockInner.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		client := &rateLimitedUserAssignedIdentitiesClient{inner: mockInner, rateLimiter: flowcontrol.NewFakeNeverRateLimiter()}

		result, err := client.Get(context.Background(), "rg", "identity", nil)
		require.Error(t, err)
		assert.Equal(t, armmsi.UserAssignedIdentitiesClientGetResponse{}, result)
	})
}

func TestUserAssignedIdentitiesRateLimiters_GetOrCreate(t *testing.T) {
	newTestRateLimiters := func() *userAssignedIdentitiesRateLimiters {
		return &userAssignedIdentitiesRateLimiters{
			// Real token buckets rather than NewFakeAlwaysRateLimiter: fakeAlwaysRateLimiter is a
			// zero-size struct, and Go's runtime gives every zero-size allocation the same address,
			// so distinct instances would compare as the same object and defeat the Same/NotSame
			// assertions below. Token buckets have distinct addresses. Their qps/burst are
			// irrelevant here: these subtests only assert limiter identity, never timing.
			newRateLimiter: func() flowcontrol.RateLimiter { return flowcontrol.NewTokenBucketRateLimiter(1, 1) },
			rateLimiters:   lru.New(userAssignedIdentitiesRateLimitersCacheSize),
		}
	}

	t.Run("returns the same rate limiter for the same principal client ID", func(t *testing.T) {
		r := newTestRateLimiters()

		first := r.getOrCreate("client-id")
		second := r.getOrCreate("client-id")
		assert.Same(t, first, second, "expected both lookups for the same principal to share one rate limiter")
	})

	t.Run("returns distinct rate limiters for distinct principal client IDs", func(t *testing.T) {
		r := newTestRateLimiters()

		first := r.getOrCreate("client-id-1")
		second := r.getOrCreate("client-id-2")
		assert.NotSame(t, first, second, "expected distinct principals to get distinct rate limiters")
	})
}
