// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
//
//
// This file contains code derived from CockroachDB. The async send message
// pattern used in ASyncSend/connectAndProcess/connectAndProcess is similar
// to the one used in CockroachDB.
//
// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

/*
Package transport implements the transport component used for exchanging
Raft messages between NodeHosts.

This package is internally used by Dragonboat, applications are not expected
to import this package.
*/
package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coufalja/tugboat-transport/config"
	"github.com/coufalja/tugboat/logger"
	"github.com/coufalja/tugboat/raftio"
	pb "github.com/coufalja/tugboat/raftpb"
	"github.com/coufalja/tugboat/rate"
	"github.com/lni/goutils/logutil"
	"github.com/lni/goutils/netutil"
	circuit "github.com/lni/goutils/netutil/rubyist/circuitbreaker"
	"github.com/lni/goutils/syncutil"
	"github.com/lni/vfs"
)

const (
	maxMsgBatchSize = 64 * 1024 * 1024
)

var (
	plog                = logger.GetLogger("transport")
	sendQueueLen        = 1024 * 2
	idleTimeout         = time.Minute
	errChunkSendSkipped = errors.New("chunk skipped")
	errBatchSendSkipped = errors.New("batch skipped")
	dn                  = logutil.DescribeNode
)

func firstError(err1, err2 error) error {
	if err1 != nil {
		return err1
	}
	return err2
}

//
// funcs used mainly in testing
//

// StreamChunkSendFunc is a func type that is used to determine whether a
// snapshot chunk should indeed be sent. This func is used in test only.
type StreamChunkSendFunc func(pb.Chunk) (pb.Chunk, bool)

// SendMessageBatchFunc is a func type that is used to determine whether the
// specified message batch should be sent. This func is used in test only.
type SendMessageBatchFunc func(pb.MessageBatch) (pb.MessageBatch, bool)

type sendQueue struct {
	ch chan pb.Message
	rl *rate.RateLimiter
}

func (sq *sendQueue) rateLimited() bool {
	return sq.rl.RateLimited()
}

func (sq *sendQueue) increase(msg pb.Message) {
	if msg.Type != pb.Replicate {
		return
	}
	sq.rl.Increase(pb.GetEntrySliceInMemSize(msg.Entries))
}

func (sq *sendQueue) decrease(msg pb.Message) {
	if msg.Type != pb.Replicate {
		return
	}
	sq.rl.Decrease(pb.GetEntrySliceInMemSize(msg.Entries))
}

type failedSend uint64

type nodeMap map[raftio.NodeInfo]struct{}

const (
	success failedSend = iota
	circuitBreakerNotReady
	unknownTarget
	rateLimited
	chanIsFull
)

// Transport is the transport layer for delivering raft messages and snapshots.
type Transport struct {
	mu struct {
		sync.Mutex
		queues   map[string]sendQueue
		breakers map[string]*circuit.Breaker
	}
	sysEvents        config.ITransportEvent
	ctx              context.Context
	preSendBatch     atomic.Value
	preSend          atomic.Value
	postSend         atomic.Value
	msgHandler       pb.IMessageHandler
	resolver         config.IResolver
	trans            raftio.ITransport
	fs               vfs.FS
	stopper          *syncutil.Stopper
	dir              func(clusterID uint64, nodeID uint64) string
	metrics          Metrics
	chunks           *Chunk
	cancel           context.CancelFunc
	sourceID         string
	jobs             uint64
	deploymentID     uint64
	maxSendQueueSize uint64
}

// NewTransport creates a new Transport object.
func NewTransport(cfg *config.Config) func(handler pb.IMessageHandler) (*Transport, error) {
	if cfg.DeploymentID == 0 {
		cfg.DeploymentID = 1
	}
	return func(handler pb.IMessageHandler) (*Transport, error) {
		t := &Transport{
			sourceID:         cfg.RaftAddress,
			deploymentID:     cfg.DeploymentID,
			resolver:         cfg.Resolver,
			stopper:          syncutil.NewStopper(),
			dir:              cfg.SnapshotDir,
			sysEvents:        cfg.SysEvents,
			fs:               cfg.FS,
			msgHandler:       handler,
			maxSendQueueSize: cfg.MaxSendQueueSize,
		}
		chunks := NewChunk(t.handleRequest, t.snapshotReceived, t.dir, cfg.DeploymentID, cfg.FS)
		t.trans = cfg.WireFactory(cfg.RaftAddress, cfg.DeploymentID, t.handleRequest, chunks.Add)
		t.chunks = chunks
		plog.Infof("transport type: %s", t.trans.Name())
		if err := t.trans.Start(); err != nil {
			plog.Errorf("transport failed to start %v", err)
			if cerr := t.trans.Close(); cerr != nil {
				plog.Errorf("failed to close the transport module %v", cerr)
			}
			return nil, err
		}
		t.stopper.RunWorker(func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					chunks.Tick()
				case <-t.stopper.ShouldStop():
					return
				}
			}
		})
		t.ctx, t.cancel = context.WithCancel(context.Background())
		t.mu.queues = make(map[string]sendQueue)
		t.mu.breakers = make(map[string]*circuit.Breaker)
		t.metrics = &NOOPMetrics{}
		return t, nil
	}
}

// Name returns the type name of the transport module.
func (t *Transport) Name() string {
	return t.trans.Name()
}

// GetTrans returns the transport instance.
func (t *Transport) GetTrans() raftio.ITransport {
	return t.trans
}

// SetPreSendBatchHook set the SendMessageBatch hook.
// This function is only expected to be used in monkey testing.
func (t *Transport) SetPreSendBatchHook(h SendMessageBatchFunc) {
	t.preSendBatch.Store(h)
}

// SetPreStreamChunkSendHook sets the StreamChunkSend hook function that will
// be called before each snapshot chunk is sent.
func (t *Transport) SetPreStreamChunkSendHook(h StreamChunkSendFunc) {
	t.preSend.Store(h)
}

// Close closes the Transport object.
func (t *Transport) Close() error {
	t.cancel()
	t.stopper.Stop()
	t.chunks.Close()
	return t.trans.Close()
}

// GetCircuitBreaker returns the circuit breaker used for the specified
// target node.
func (t *Transport) GetCircuitBreaker(key string) *circuit.Breaker {
	t.mu.Lock()
	breaker, ok := t.mu.breakers[key]
	if !ok {
		breaker = netutil.NewBreaker()
		t.mu.breakers[key] = breaker
	}
	t.mu.Unlock()

	return breaker
}

func (t *Transport) handleRequest(req pb.MessageBatch) {
	if req.DeploymentId != t.deploymentID {
		plog.Warningf("deployment id does not match %d vs %d, message dropped",
			req.DeploymentId, t.deploymentID)
		return
	}
	if req.BinVer != raftio.TransportBinVersion {
		plog.Warningf("binary compatibility version not match %d vs %d",
			req.BinVer, raftio.TransportBinVersion)
		return
	}
	addr := req.SourceAddress
	if len(addr) > 0 {
		for _, r := range req.Requests {
			if r.From != 0 {
				t.resolver.Add(r.ClusterId, r.From, addr)
			}
		}
	}
	ssCount, msgCount := t.msgHandler.HandleMessageBatch(req)
	dropedMsgCount := uint64(len(req.Requests)) - ssCount - msgCount
	t.metrics.ReceivedMessages(ssCount, msgCount, dropedMsgCount)
}

func (t *Transport) snapshotReceived(clusterID uint64,
	nodeID uint64, from uint64) {
	t.msgHandler.HandleSnapshot(clusterID, nodeID, from)
}

func (t *Transport) notifyUnreachable(addr string, affected nodeMap) {
	plog.Warningf("%s became unreachable, affected %d nodes", addr, len(affected))
	for n := range affected {
		t.msgHandler.HandleUnreachable(n.ClusterID, n.NodeID)
	}
}

// Send asynchronously sends raft messages to their target nodes.
//
// The generic async send Go pattern used in Send() is found in CockroachDB's
// codebase.
func (t *Transport) Send(req pb.Message) bool {
	v, _ := t.send(req)
	if !v {
		t.metrics.MessageSendFailure(1)
	}
	return v
}

func (t *Transport) send(req pb.Message) (bool, failedSend) {
	if req.Type == pb.InstallSnapshot {
		panic("snapshot message must be sent via its own channel.")
	}
	toNodeID := req.To
	clusterID := req.ClusterId
	from := req.From
	addr, key, err := t.resolver.Resolve(clusterID, toNodeID)
	if err != nil {
		plog.Warningf("%s do not have the address for %s, dropping a message",
			t.sourceID, dn(clusterID, toNodeID))
		return false, unknownTarget
	}
	// fail fast
	if !t.GetCircuitBreaker(addr).Ready() {
		t.metrics.MessageConnectionFailure()
		return false, circuitBreakerNotReady
	}
	// get the channel, create it in case it is not in the queue map
	t.mu.Lock()
	sq, ok := t.mu.queues[key]
	if !ok {
		sq = sendQueue{
			ch: make(chan pb.Message, sendQueueLen),
			rl: rate.NewRateLimiter(t.maxSendQueueSize),
		}
		t.mu.queues[key] = sq
	}
	t.mu.Unlock()
	if !ok {
		shutdownQueue := func() {
			t.mu.Lock()
			delete(t.mu.queues, key)
			t.mu.Unlock()
		}
		t.stopper.RunWorker(func() {
			affected := make(nodeMap)
			if !t.connectAndProcess(addr, sq, from, affected) {
				t.notifyUnreachable(addr, affected)
			}
			shutdownQueue()
		})
	}
	if sq.rateLimited() {
		return false, rateLimited
	}
	select {
	case sq.ch <- req:
		sq.increase(req)
		return true, success
	default:
		return false, chanIsFull
	}
}

// connectAndProcess returns a boolean value indicating whether it is stopped
// gracefully when the system is being shutdown.
func (t *Transport) connectAndProcess(remoteHost string,
	sq sendQueue, from uint64, affected nodeMap) bool {
	breaker := t.GetCircuitBreaker(remoteHost)
	successes := breaker.Successes()
	consecFailures := breaker.ConsecFailures()
	if err := func() error {
		plog.Debugf("%s is trying to connect to %s", t.sourceID, remoteHost)
		conn, err := t.trans.GetConnection(t.ctx, remoteHost)
		if err != nil {
			plog.Errorf("Nodehost %s failed to get a connection to %s, %v",
				t.sourceID, remoteHost, err)
			return err
		}
		defer conn.Close()
		breaker.Success()
		if successes == 0 || consecFailures > 0 {
			plog.Debugf("message streaming to %s established", remoteHost)
			t.sysEvents.ConnectionEstablished(remoteHost, false)
		}
		return t.processMessages(remoteHost, sq, conn, affected)
	}(); err != nil {
		plog.Warningf("breaker %s to %s failed, connect and process failed: %s",
			t.sourceID, remoteHost, err.Error())
		breaker.Fail()
		t.metrics.MessageConnectionFailure()
		t.sysEvents.ConnectionFailed(remoteHost, false)
		return false
	}
	return true
}

func (t *Transport) processMessages(remoteHost string,
	sq sendQueue, conn raftio.IConnection, affected nodeMap) error {
	idleTimer := time.NewTimer(idleTimeout)
	defer idleTimer.Stop()
	sz := uint64(0)
	batch := pb.MessageBatch{
		SourceAddress: t.sourceID,
		BinVer:        raftio.TransportBinVersion,
	}
	did := t.deploymentID
	requests := make([]pb.Message, 0)
	for {
		idleTimer.Reset(idleTimeout)
		select {
		case <-t.stopper.ShouldStop():
			return nil
		case <-idleTimer.C:
			return nil
		case req := <-sq.ch:
			n := raftio.NodeInfo{
				ClusterID: req.ClusterId,
				NodeID:    req.From,
			}
			affected[n] = struct{}{}
			sq.decrease(req)
			sz += uint64(req.SizeUpperLimit())
			requests = append(requests, req)
			for done := false; !done && sz < maxMsgBatchSize; {
				select {
				case req = <-sq.ch:
					sq.decrease(req)
					sz += uint64(req.SizeUpperLimit())
					requests = append(requests, req)
				case <-t.stopper.ShouldStop():
					return nil
				default:
					done = true
				}
			}
			batch.DeploymentId = did
			twoBatch := false
			if sz < maxMsgBatchSize || len(requests) == 1 {
				batch.Requests = requests
			} else {
				twoBatch = true
				batch.Requests = requests[:len(requests)-1]
			}
			if err := t.sendMessageBatch(conn, batch); err != nil {
				plog.Errorf("send batch failed, target %s (%v), %d",
					remoteHost, err, len(batch.Requests))
				return err
			}
			if twoBatch {
				batch.Requests = []pb.Message{requests[len(requests)-1]}
				if err := t.sendMessageBatch(conn, batch); err != nil {
					plog.Errorf("send batch failed, taret node %s (%v), %d",
						remoteHost, err, len(batch.Requests))
					return err
				}
			}
			sz = 0
			requests, batch = lazyFree(requests, batch)
			requests = requests[:0]
		}
	}
}

func lazyFree(reqs []pb.Message, mb pb.MessageBatch) ([]pb.Message, pb.MessageBatch) {
	for i := 0; i < len(reqs); i++ {
		reqs[i].Entries = nil
	}
	mb.Requests = []pb.Message{}
	return reqs, mb
}

func (t *Transport) sendMessageBatch(conn raftio.IConnection,
	batch pb.MessageBatch) error {
	if f := t.preSendBatch.Load(); f != nil {
		updated, shouldSend := f.(SendMessageBatchFunc)(batch)
		if !shouldSend {
			return errBatchSendSkipped
		}
		return conn.SendMessageBatch(updated)
	}
	if err := conn.SendMessageBatch(batch); err != nil {
		t.metrics.MessageSendFailure(uint64(len(batch.Requests)))
		return err
	}
	t.metrics.MessageSendSuccess(uint64(len(batch.Requests)))
	return nil
}
