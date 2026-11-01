// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"testing"
	"time"
)

// TestRaft_LeadershipTransferInterrupted interrupts a leadership transfer
// in three ways and measures how long the old leader refuses writes
// (ErrLeadershipTransferInProgress) from the start of the transfer, and
// when the transfer's future answers. Upstream issue 696.
func TestRaft_LeadershipTransferInterrupted(t *testing.T) {
	cases := []struct {
		name string
		// cut interrupts the transfer to target, right after it started
		// (before: true, before it started), on leader l's transport.
		cut    func(c *cluster, l *Raft, target *Raft)
		before bool
		// max bounds the refusal window.
		max func(conf *Config, transTimeout time.Duration) time.Duration
	}{
		{
			// The target is unreachable: TimeoutNow fails at once.
			name: "partitioned",
			cut: func(c *cluster, l *Raft, target *Raft) {
				c.Disconnect(target.localAddr)
			},
			before: true,
			max:    func(conf *Config, _ time.Duration) time.Duration { return conf.ElectionTimeout },
		},
		{
			// The target answers TimeoutNow, then dies before campaigning:
			// the leader waits another ElectionTimeout to lose the lead.
			name: "target dies after TimeoutNow",
			cut: func(c *cluster, l *Raft, target *Raft) {
				l.trans.(*InmemTransport).Connect(target.localAddr, fakePeer(target.localAddr, true))
			},
			before: true,
			max:    func(conf *Config, _ time.Duration) time.Duration { return 2*conf.ElectionTimeout + conf.ElectionTimeout },
		},
		{
			// The target hangs (accepts RPCs, never answers): TimeoutNow
			// blocks until the transport's timeout (10 x 500 ms in memory;
			// up to the dial timeout + the RPC deadline on the network).
			name: "target hangs",
			cut: func(c *cluster, l *Raft, target *Raft) {
				l.trans.(*InmemTransport).Connect(target.localAddr, fakePeer(target.localAddr, false))
			},
			before: true,
			max:    func(conf *Config, tt time.Duration) time.Duration { return 10*tt + conf.ElectionTimeout },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := MakeCluster(3, t, nil)
			defer c.Close()
			l := c.Leader()
			if err := l.Apply([]byte("x"), time.Second).Error(); err != nil {
				t.Fatal(err)
			}
			c.WaitForReplication(1)
			var target *Raft
			for _, f := range c.Followers() {
				target = f
				break
			}
			conf := l.config()

			if tc.before {
				tc.cut(c, l, target)
			}
			start := time.Now()
			fut := l.LeadershipTransferToServer(target.localID, target.localAddr)
			futDone := make(chan time.Duration, 1)
			go func() { _ = fut.Error(); futDone <- time.Since(start) }()

			refused := false
			var accepted time.Duration
			for time.Since(start) < 30*time.Second {
				err := l.Apply([]byte("w"), 20*time.Millisecond).Error()
				if err == nil {
					accepted = time.Since(start)
					break
				}
				if errors.Is(err, ErrLeadershipTransferInProgress) {
					refused = true
				} else if l.State() != Leader {
					t.Fatalf("leader lost the lead (%v) after %v", err, time.Since(start))
				}
				time.Sleep(time.Millisecond)
			}
			futAt := <-futDone
			t.Logf("%s: future answered %v (%v) after %v; writes refused=%v, accepted again after %v (ElectionTimeout %v)",
				tc.name, fut.Error(), l.State(), futAt, refused, accepted, conf.ElectionTimeout)
			if accepted == 0 {
				t.Fatalf("writes still refused after 30 s")
			}
			if bound := tc.max(&conf, 500*time.Millisecond) + 250*time.Millisecond; accepted > bound {
				t.Fatalf("writes refused for %v, beyond %v", accepted, bound)
			}
		})
	}
}

// fakePeer is an in-memory transport at addr that answers TimeoutNow with
// success (if answer) and leaves every other RPC unanswered.
func fakePeer(addr ServerAddress, answer bool) *InmemTransport {
	_, tr := NewInmemTransportWithTimeout(addr, 500*time.Millisecond)
	go func() {
		for rpc := range tr.consumerCh {
			if _, ok := rpc.Command.(*TimeoutNowRequest); ok && answer {
				rpc.Respond(&TimeoutNowResponse{}, nil)
			}
		}
	}()
	return tr
}
