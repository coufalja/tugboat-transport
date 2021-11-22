// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

package noop

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coufalja/tugboat/raftio"
	"github.com/coufalja/tugboat/raftpb"
)

var (
	// Name is the module name for the NOOP transport module.
	Name = "noop-test-transport"
	// ErrRequestedToFail is the error used to indicate that the error is
	// requested.
	ErrRequestedToFail = errors.New("requested to returned error")
)

type Request struct {
	mu      sync.Mutex
	fail    bool
	blocked bool
}

func (r *Request) SetToFail(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = v
}

func (r *Request) Fail() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail
}

func (r *Request) SetBlocked(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blocked = v
}

func (r *Request) Blocked() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.blocked
}

type ConnectRequest struct {
	mu   sync.Mutex
	fail bool
}

func (r *ConnectRequest) SetToFail(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = v
}

func (r *ConnectRequest) Fail() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail
}

// Connection is the connection used to exchange messages between node hosts.
type Connection struct {
	req *Request
}

// Close closes the Connection instance.
func (c *Connection) Close() {
}

// SendMessageBatch return ErrRequestedToFail when requested.
func (c *Connection) SendMessageBatch(batch raftpb.MessageBatch) error {
	if c.req.Fail() {
		return ErrRequestedToFail
	}
	for c.req.Blocked() {
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// SnapshotConnection is the connection used to send snapshots.
type SnapshotConnection struct {
	Req             *Request
	sendChunksCount uint64
}

// Close closes the SnapshotConnection.
func (c *SnapshotConnection) Close() {
}

// SendChunk returns ErrRequestedToFail when requested.
func (c *SnapshotConnection) SendChunk(chunk raftpb.Chunk) error {
	if c.Req.Fail() {
		return ErrRequestedToFail
	}
	c.sendChunksCount++
	return nil
}

// Transport is a transport module for testing purposes. It does not
// actually has the ability to exchange messages or snapshots between
// nodehosts.
type Transport struct {
	Req        *Request
	ConnReq    *ConnectRequest
	Connected  uint64
	TryConnect uint64
}

// NewNOOPTransport creates a new Transport instance.
func NewNOOPTransport() *Transport {
	return &Transport{
		Req:     &Request{},
		ConnReq: &ConnectRequest{},
	}
}

// Start starts the Transport instance.
func (g *Transport) Start() error {
	return nil
}

// Close closes the Transport instance.
func (g *Transport) Close() error {
	return nil
}

// GetConnection returns a connection.
func (g *Transport) GetConnection(ctx context.Context,
	target string) (raftio.IConnection, error) {
	atomic.AddUint64(&g.TryConnect, 1)
	if g.ConnReq.Fail() {
		return nil, ErrRequestedToFail
	}
	atomic.AddUint64(&g.Connected, 1)
	return &Connection{req: g.Req}, nil
}

// GetSnapshotConnection returns a snapshot connection.
func (g *Transport) GetSnapshotConnection(ctx context.Context,
	target string) (raftio.ISnapshotConnection, error) {
	atomic.AddUint64(&g.TryConnect, 1)
	if g.ConnReq.Fail() {
		return nil, ErrRequestedToFail
	}
	atomic.AddUint64(&g.Connected, 1)
	return &SnapshotConnection{Req: g.Req}, nil
}

// Name returns the module name.
func (g *Transport) Name() string {
	return Name
}
