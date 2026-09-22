/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package grpcgcp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GoogleCloudPlatform/grpc-gcp-go/grpcgcp/mocks"
	"github.com/golang/mock/gomock"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Returns a probe function yielding the given results in order and repeating
// the last one once exhausted.
func probeScript(results ...string) GCPFallbackProbeFn {
	calls := &atomic.Int64{}
	return func(grpc.ClientConnInterface) string {
		i := int(calls.Add(1)) - 1
		if i >= len(results) {
			i = len(results) - 1
		}
		return results[i]
	}
}

// The rate check and the primary probe are given different periods so that
// their ticks never coincide; the order of two goroutines woken by the same
// tick is unspecified.
func recoveryOpts(probe GCPFallbackProbeFn) *GCPFallbackOptions {
	opts := NewGCPFallbackOptions()
	opts.Period = 10 * time.Second
	opts.ErrorRateThreshold = 0.5
	opts.MinFailedCalls = 3
	opts.PrimaryProbingInterval = 3 * time.Second
	opts.PrimaryProbingFn = probe
	opts.EnableRecovery = true
	opts.MinPrimaryProbeSuccessCount = 3
	// Most tests only exercise the count; the duration has its own tests.
	opts.MinPrimaryProbeSuccessDuration = 0
	return opts
}

// Makes n failing calls on the primary connection, enough for the next rate
// check to fall back with the options from recoveryOpts.
func failPrimary(t *testing.T, f *GCPFallback, primaryConn *mocks.MockClientConnInterface, n int) {
	t.Helper()
	expectUnary(t, primaryConn, codes.Unavailable).Times(n)
	for i := 0; i < n; i++ {
		f.Invoke(context.Background(), "method", nil, nil)
	}
}

func wantFallback(t *testing.T, f *GCPFallback, want bool, when string) {
	t.Helper()
	if got := f.state.isInFallback(); got != want {
		t.Fatalf("isInFallback() = %v, want %v (%s)", got, want, when)
	}
}

func TestGCPFallbackRecovery_DisabledByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := recoveryOpts(probeScript(""))
		opts.EnableRecovery = NewGCPFallbackOptions().EnableRecovery
		_, gcpFallback, primaryConn, _ := setup(t, opts)

		failPrimary(t, gcpFallback, primaryConn, 3)

		time.Sleep(11 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, true, "after rate check")

		// Plenty of successful probes, none of which may recover.
		time.Sleep(30 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, true, "recovery disabled")
	})
}

func TestGCPFallbackRecovery_FullCycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, gcpFallback, primaryConn, fallbackConn := setup(t, recoveryOpts(probeScript("")))

		// The tickers are not restarted between cycles, so the timeline is
		// absolute rather than relative to each cycle.
		start := time.Now()
		at := func(d time.Duration) {
			time.Sleep(time.Until(start.Add(d)))
			synctest.Wait()
		}

		failPrimary(t, gcpFallback, primaryConn, 3)

		at(11 * time.Second) // Rate check at t=10s.
		wantFallback(t, gcpFallback, true, "first outage")
		expectUnary(t, fallbackConn, codes.OK).Times(1)
		gcpFallback.Invoke(context.Background(), "method", nil, nil)

		at(16 * time.Second) // Probes at t=12s and t=15s, one short.
		wantFallback(t, gcpFallback, true, "two successful probes")

		at(19 * time.Second) // Third probe at t=18s.
		wantFallback(t, gcpFallback, false, "first recovery")
		expectUnary(t, primaryConn, codes.OK).Times(1)
		gcpFallback.Invoke(context.Background(), "method", nil, nil)

		// The primary can go bad again after a recovery.
		failPrimary(t, gcpFallback, primaryConn, 3)

		at(21 * time.Second) // Rate check at t=20s.
		wantFallback(t, gcpFallback, true, "second outage")
		expectUnary(t, fallbackConn, codes.OK).Times(1)
		gcpFallback.Invoke(context.Background(), "method", nil, nil)

		at(31 * time.Second) // Probes at t=21s, 24s and 27s.
		wantFallback(t, gcpFallback, false, "second recovery")
		expectUnary(t, primaryConn, codes.OK).Times(1)
		gcpFallback.Invoke(context.Background(), "method", nil, nil)
	})
}

func TestGCPFallbackRecovery_ProbeFailureResetsProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := recoveryOpts(probeScript("", "", "Unavailable", ""))
		// A long interval keeps the timeline easy to follow.
		opts.PrimaryProbingInterval = 30 * time.Second
		_, gcpFallback, primaryConn, _ := setup(t, opts)

		failPrimary(t, gcpFallback, primaryConn, 3)

		time.Sleep(11 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, true, "after rate check")

		// t=101s: probes at t=30s, 60s and 90s, the last of which failed.
		time.Sleep(90 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, true, "two successes then a failure")

		// t=151s: probes at t=120s and t=150s succeed, still one short.
		time.Sleep(50 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, true, "two successes after the reset")

		// t=181s: the probe at t=180s completes the new run of three.
		time.Sleep(30 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, false, "three successes after the reset")
	})
}

func TestGCPFallbackRecovery_MinDurationBlocksRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := recoveryOpts(probeScript(""))
		opts.MinPrimaryProbeSuccessCount = 1
		opts.MinPrimaryProbeSuccessDuration = 30 * time.Second
		_, gcpFallback, primaryConn, _ := setup(t, opts)

		failPrimary(t, gcpFallback, primaryConn, 3)

		time.Sleep(11 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, true, "after rate check")

		// t=40s: the count was met by the probe at t=12s, but the probes have
		// only been succeeding for 27s.
		time.Sleep(29 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, true, "duration not met")

		// t=43s: the probe at t=42s is 30s after the first success.
		time.Sleep(3 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, false, "duration met")
	})
}

// Calls already in flight on the primary when the fallback happens report their
// failures afterwards. They describe the outage, so they are not counted, and
// must not trigger another fallback after the recovery.
func TestGCPFallbackRecovery_NoImmediateRefallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, gcpFallback, primaryConn, _ := setup(t, recoveryOpts(probeScript("")))

		failPrimary(t, gcpFallback, primaryConn, 3)

		time.Sleep(11 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, true, "after rate check")

		// Stragglers from before the fallback.
		for i := 0; i < 5; i++ {
			gcpFallback.reportPrimaryStatus(context.Background(), codes.Unavailable)
		}
		if successes, failures := gcpFallback.state.drainPrimary(); successes+failures != 0 {
			t.Errorf("primary counters = %d successes, %d failures in fallback, want 0", successes, failures)
		}

		time.Sleep(8 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, false, "after successful probes")

		// t=31s: the rate checks at t=20s and t=30s saw no traffic.
		time.Sleep(12 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, false, "stale failures dropped")
	})
}

// A recovery is a fallback in the opposite direction and is counted as such.
func TestGCPFallbackRecovery_Metric(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reader := metric.NewManualReader()
		opts := recoveryOpts(probeScript(""))
		opts.MeterProvider = metric.NewMeterProvider(metric.WithReader(reader))
		_, gcpFallback, primaryConn, _ := setup(t, opts)

		failPrimary(t, gcpFallback, primaryConn, 3)

		time.Sleep(19 * time.Second)
		synctest.Wait()
		wantFallback(t, gcpFallback, false, "after successful probes")

		// Further successful probes must not be counted again.
		time.Sleep(20 * time.Second)
		synctest.Wait()

		want := metricdata.Metrics{
			Name:        "eef.fallback_count",
			Description: "Number of fallbacks occurred from one channel to another.",
			Unit:        "{occurrence}",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{Value: 1, Attributes: attribute.NewSet(
						attribute.String("from_channel_name", "primary"),
						attribute.String("to_channel_name", "fallback"),
					)},
					{Value: 1, Attributes: attribute.NewSet(
						attribute.String("from_channel_name", "fallback"),
						attribute.String("to_channel_name", "primary"),
					)},
				},
				IsMonotonic: true,
				Temporality: metricdata.CumulativeTemporality,
			},
		}

		got, ok := metricsDataFromReader(context.Background(), t, reader)[want.Name]
		if !ok {
			t.Fatalf("Metric %v not present in recorded metrics", want.Name)
		}
		if !metricdatatest.AssertEqual(t, want, got, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars()) {
			t.Fatalf("Metrics data not equal for metric: %v", want.Name)
		}
	})
}

// Instances sharing a state fall back and recover together, and their call
// results are pooled into one decision.
func TestGCPFallbackState_Shared(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := &GCPFallbackState{} // The zero value is usable.
		t.Cleanup(state.Close)

		opts := recoveryOpts(probeScript(""))
		opts.SharedState = state

		_, fallbackA, primaryA, _ := setup(t, opts)
		_, fallbackB, primaryB, fallbackConnB := setup(t, opts)

		// Neither instance reaches MinFailedCalls alone; together they do.
		failPrimary(t, fallbackA, primaryA, 2)
		failPrimary(t, fallbackB, primaryB, 1)

		time.Sleep(11 * time.Second)
		synctest.Wait()
		wantFallback(t, fallbackA, true, "instance A after rate check")
		wantFallback(t, fallbackB, true, "instance B after rate check")

		expectUnary(t, fallbackConnB, codes.OK).Times(1)
		fallbackB.Invoke(context.Background(), "method", nil, nil)

		// Probe progress is pooled, so the two instances together meet
		// MinPrimaryProbeSuccessCount of 3 with the probes at t=12s and t=15s.
		// Per-instance progress would need a third round at t=18s.
		time.Sleep(5 * time.Second)
		synctest.Wait()
		wantFallback(t, fallbackA, false, "instance A after successful probes")
		wantFallback(t, fallbackB, false, "instance B after successful probes")
	})
}

// The rate check belongs to the state, so closing the instance that started it
// must not stop it for the others.
func TestGCPFallbackState_SharedSurvivesMemberClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := NewGCPFallbackState()
		t.Cleanup(state.Close)

		opts := NewGCPFallbackOptions()
		opts.Period = 10 * time.Second
		opts.ErrorRateThreshold = 0.5
		opts.MinFailedCalls = 3
		opts.SharedState = state

		_, fallbackA, _, _ := setup(t, opts)
		_, fallbackB, primaryB, _ := setup(t, opts)

		// A attached first, so A started the rate check.
		fallbackA.Close()

		failPrimary(t, fallbackB, primaryB, 3)

		time.Sleep(11 * time.Second)
		synctest.Wait()
		wantFallback(t, fallbackB, true, "rate check outlived instance A")
	})
}

// An instance with fallback disabled must not take the rate check of a shared
// state, which would disable fallback for every instance sharing it.
func TestGCPFallbackState_DisabledMemberDoesNotTakeRateCheck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := NewGCPFallbackState()
		t.Cleanup(state.Close)

		opts := NewGCPFallbackOptions()
		opts.Period = 10 * time.Second
		opts.ErrorRateThreshold = 0.5
		opts.MinFailedCalls = 3
		opts.SharedState = state
		opts.EnableFallback = false
		setup(t, opts)

		opts.EnableFallback = true
		_, fallbackB, primaryB, _ := setup(t, opts)

		failPrimary(t, fallbackB, primaryB, 3)

		time.Sleep(11 * time.Second)
		synctest.Wait()
		wantFallback(t, fallbackB, true, "after rate check")
	})
}

func TestGCPFallbackState_RateCheckLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := NewGCPFallbackState()

		var zero, first, second atomic.Int64
		state.startRateCheck(0, func() { zero.Add(1) }) // Ignored, not a panic.
		state.startRateCheck(time.Second, func() { first.Add(1) })
		state.startRateCheck(time.Second, func() { second.Add(1) })

		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()
		state.Close()
		state.Close() // Idempotent.
		state.startRateCheck(time.Second, func() { second.Add(1) })

		time.Sleep(3 * time.Second)
		synctest.Wait()
		if got := zero.Load(); got != 0 {
			t.Errorf("check with a zero period ran %d times, want 0", got)
		}
		if got := first.Load(); got != 3 {
			t.Errorf("first check ran %d times, want 3 (stopped by Close)", got)
		}
		if got := second.Load(); got != 0 {
			t.Errorf("second check ran %d times, want 0", got)
		}
	})
}

func TestGCPFallbackState_Generation(t *testing.T) {
	state := NewGCPFallbackState()
	defer state.Close()

	cfg := recoveryConfig{enabled: true, minSuccesses: 1}

	// Read as the rate check and the probes do, before the state moves on.
	stale := state.generation.Load()
	if !state.triggerFallback(stale) {
		t.Fatal("triggerFallback() = false, want true")
	}
	generation := state.generation.Load()

	// A no-op fallback must not bump the generation, which would reset the
	// recovery progress of every instance sharing the state.
	if state.triggerFallback(generation) {
		t.Error("triggerFallback() while already in fallback = true, want false")
	}
	if got := state.generation.Load(); got != generation {
		t.Errorf("generation = %d after a no-op fallback, want %d", got, generation)
	}

	if state.recordPrimaryProbeResult(true, stale, cfg) {
		t.Error("stale probe result recovered, want it ignored")
	}
	if !state.isInFallback() {
		t.Fatal("isInFallback() = false after a stale probe result, want true")
	}
	if !state.recordPrimaryProbeResult(true, generation, cfg) {
		t.Error("current probe result did not recover")
	}

	// The same guard covers the fallback path: statistics collected before the
	// recovery describe an outage that is over.
	if state.triggerFallback(generation) {
		t.Error("triggerFallback() with a stale generation = true, want false")
	}
}

// Drives the RPC path from several goroutines while the rate check and the
// probes flap the mode underneath it. Asserts no outcome; it exists to be run
// under -race, and to catch a deadlock if the transitions ever start taking a
// lock that the RPC path also needs.
func TestGCPFallbackRecovery_ConcurrentRPCs(t *testing.T) {
	ctrl := gomock.NewController(t)
	primaryConn := mocks.NewMockClientConnInterface(ctrl)
	fallbackConn := mocks.NewMockClientConnInterface(ctrl)

	// The primary always fails and its probe always succeeds, so the state
	// falls back and recovers continuously.
	primaryConn.EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(status.Error(codes.Unavailable, "unavailable")).AnyTimes()
	fallbackConn.EXPECT().Invoke(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()

	opts := NewGCPFallbackOptions()
	opts.Period = time.Millisecond
	opts.MinFailedCalls = 1
	opts.PrimaryProbingInterval = time.Millisecond
	opts.PrimaryProbingFn = probeScript("")
	opts.EnableRecovery = true
	opts.MinPrimaryProbeSuccessCount = 1
	opts.MinPrimaryProbeSuccessDuration = 0

	gcpFallback, err := NewGCPFallback(context.Background(), primaryConn, fallbackConn, opts)
	if err != nil {
		t.Fatalf("NewGCPFallback() error = %v", err)
	}
	t.Cleanup(gcpFallback.Close)

	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					gcpFallback.Invoke(context.Background(), "method", nil, nil)
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(done)
	wg.Wait()
}

func TestNewGCPFallback_RejectsInvalidRecoveryOptions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*GCPFallbackOptions)
	}{
		{"recovery without a probe", func(o *GCPFallbackOptions) { o.EnableRecovery = true }},
		{"negative count", func(o *GCPFallbackOptions) { o.MinPrimaryProbeSuccessCount = -1 }},
		{"negative duration", func(o *GCPFallbackOptions) { o.MinPrimaryProbeSuccessDuration = -time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := NewGCPFallbackOptions()
			tc.apply(opts)
			if _, err := NewGCPFallback(context.Background(), nil, nil, opts); err == nil {
				t.Error("NewGCPFallback() error = nil, want an error")
			}
		})
	}
}

// With a shared state, one round of probes over enough instances meets
// MinPrimaryProbeSuccessCount. The default MinPrimaryProbeSuccessDuration keeps
// that from recovering the pool before the primary has been healthy a while.
func TestGCPFallbackState_SharedRecoveryNeedsDefaultDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := NewGCPFallbackState()
		t.Cleanup(state.Close)

		opts := recoveryOpts(probeScript(""))
		opts.SharedState = state
		opts.MinPrimaryProbeSuccessCount = 2
		opts.MinPrimaryProbeSuccessDuration = NewGCPFallbackOptions().MinPrimaryProbeSuccessDuration

		_, fallbackA, primaryA, _ := setup(t, opts)
		setup(t, opts)

		failPrimary(t, fallbackA, primaryA, 3)

		// Rate check at t=10s, then both instances probe at t=12s.
		time.Sleep(13 * time.Second)
		synctest.Wait()
		wantFallback(t, fallbackA, true, "one round of probes")

		// t=193s: the probes at t=192s are 3m after the first success.
		time.Sleep(180 * time.Second)
		synctest.Wait()
		wantFallback(t, fallbackA, false, "default duration met")
	})
}

// Cancelling the context of a GCPFallback closes the state it owns.
// synctest fails the test if the rate check goroutine leaks.
func TestGCPFallbackState_OwnedStopsWithContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx, cancel := context.WithCancel(context.Background())
		if _, err := NewGCPFallback(ctx, mocks.NewMockClientConnInterface(ctrl), mocks.NewMockClientConnInterface(ctrl), NewGCPFallbackOptions()); err != nil {
			t.Fatalf("NewGCPFallback() error = %v", err)
		}
		cancel()
	})
}
