/*
 *
 * Copyright 2025 gRPC authors.
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
	"time"
)

// GCPFallbackState holds the fallback mode and the call statistics behind it.
// A single state can be shared by multiple GCPFallback instances so that they
// fall back and recover together. A GCPFallback creates its own state when
// none is provided.
//
// Both transitions are currently pool-wide. Recovering per instance while
// falling back together would be an additional option, not a change to this
// type's public surface.
//
// The periodic error rate check runs once per state, using the options of the
// GCPFallback that attached first. Instances sharing a state should be
// configured with the same error rate options.
//
// All methods are safe for concurrent use.
type GCPFallbackState struct {
	// inFallback and generation are written only under mu, always together, but
	// are read without it: inFallback on the RPC hot path, and generation by
	// the probes. Every change of one is a change of the other, so an unchanged
	// generation means the mode did not move.
	inFallback atomic.Bool
	generation atomic.Uint64

	primarySuccesses  atomic.Uint64
	primaryFailures   atomic.Uint64
	fallbackSuccesses atomic.Uint64
	fallbackFailures  atomic.Uint64

	// mu serializes the fallback and recovery transitions and guards the fields
	// below.
	mu                sync.Mutex
	probeSuccesses    uint64
	firstProbeSuccess time.Time
	rateCheckStarted  bool

	ctx    context.Context
	cancel context.CancelFunc
}

// recoveryConfig is the part of GCPFallbackOptions that drives recovery. It is
// passed per call so that the state holds no per-instance configuration.
type recoveryConfig struct {
	enabled      bool
	minSuccesses uint64
	minDuration  time.Duration
}

// NewGCPFallbackState creates a state to share between GCPFallback instances
// through GCPFallbackOptions.SharedState. The caller must close it.
func NewGCPFallbackState() *GCPFallbackState {
	return newGCPFallbackState(context.Background())
}

func newGCPFallbackState(parent context.Context) *GCPFallbackState {
	ctx, cancel := context.WithCancel(parent)
	return &GCPFallbackState{ctx: ctx, cancel: cancel}
}

// isInFallback reports whether the fallback connection should be used. Kept
// unexported because per-channel recovery would make this the pool-wide
// answer rather than any single instance's.
func (s *GCPFallbackState) isInFallback() bool {
	return s.inFallback.Load()
}

// Close stops the periodic error rate check. It does not close any connection
// and does not stop the probing of the GCPFallback instances using this state.
func (s *GCPFallbackState) Close() {
	s.cancel()
}

func (s *GCPFallbackState) recordPrimary(failure bool) {
	if failure {
		s.primaryFailures.Add(1)
		return
	}
	s.primarySuccesses.Add(1)
}

func (s *GCPFallbackState) recordFallback(failure bool) {
	if failure {
		s.fallbackFailures.Add(1)
		return
	}
	s.fallbackSuccesses.Add(1)
}

// drainPrimary reads and resets the primary counters, closing an error rate window.
func (s *GCPFallbackState) drainPrimary() (successes, failures uint64) {
	return s.primarySuccesses.Swap(0), s.primaryFailures.Swap(0)
}

// drainFallback reads and resets the fallback counters.
func (s *GCPFallbackState) drainFallback() (successes, failures uint64) {
	return s.fallbackSuccesses.Swap(0), s.fallbackFailures.Swap(0)
}

// triggerFallback switches to the fallback connection and reports whether this
// call made the switch, so that only one caller reports the transition.
//
// expectedGeneration must be the generation read before the call statistics
// were collected. A mismatch means a recovery happened while they were being
// collected, which makes them describe an outage that is already over.
//
// The generation is bumped only on a real transition. Bumping it on every call
// would reset the recovery progress and could prevent recovery entirely.
func (s *GCPFallbackState) triggerFallback(expectedGeneration uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.generation.Load() != expectedGeneration || s.inFallback.Load() {
		return false
	}
	s.resetProbeProgressLocked()
	s.generation.Add(1)
	s.inFallback.Store(true)
	return true
}

// recordPrimaryProbeResult feeds a primary probe result into the recovery
// decision and reports whether it switched back to the primary connection.
//
// expectedGeneration must be the generation read before the probe ran. A
// mismatch means the mode changed while the probe was in flight, so the result
// no longer applies.
func (s *GCPFallbackState) recordPrimaryProbeResult(success bool, expectedGeneration uint64, cfg recoveryConfig) bool {
	if !cfg.enabled {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.generation.Load() != expectedGeneration || !s.inFallback.Load() {
		return false
	}
	if !success {
		s.resetProbeProgressLocked()
		return false
	}

	now := time.Now()
	if s.firstProbeSuccess.IsZero() {
		s.firstProbeSuccess = now
	}
	s.probeSuccesses++
	if s.probeSuccesses < cfg.minSuccesses || now.Sub(s.firstProbeSuccess) < cfg.minDuration {
		return false
	}

	// Drop the current error rate window. It was filled during the outage and
	// would otherwise trigger a fallback again on the next check.
	s.primarySuccesses.Store(0)
	s.primaryFailures.Store(0)
	s.resetProbeProgressLocked()
	s.generation.Add(1)
	s.inFallback.Store(false)
	return true
}

func (s *GCPFallbackState) resetProbeProgressLocked() {
	s.probeSuccesses = 0
	s.firstProbeSuccess = time.Time{}
}

// startRateCheck starts the periodic error rate check unless it is already
// running, and reports whether this call started it. The check belongs to the
// state so that it keeps running when one of the instances sharing the state
// is closed.
func (s *GCPFallbackState) startRateCheck(period time.Duration, check func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rateCheckStarted {
		return false
	}
	// Set under the same lock that starts the goroutine, so the flag can never
	// be set without a running check behind it.
	s.rateCheckStarted = true

	go func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				check()
			}
		}
	}()
	return true
}
