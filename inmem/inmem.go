// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

package inmem

import (
	"context"
	"errors"
	"sync"

	"github.com/coufalja/tugboat/config"
	"github.com/coufalja/tugboat/raftio"
	pb "github.com/coufalja/tugboat/raftpb"
	"github.com/lni/goutils/syncutil"
)

var (
	// ErrClosed indicates that the connection has been closed.
	ErrClosed = errors.New("connection closed")
	// ErrFailedToConnect indicates that connecting to the remote failed.
	ErrFailedToConnect = errors.New("failed to connect")
)

type acceptChanConn struct {
	ac  chan chanConn
	acc chan struct{}
}

// the terms send/receive are all from user's pow in this file.
type chanConn struct {
	snapshot     bool
	senderClosed chan struct{}
	recverClosed chan struct{}
	dataChan     chan []byte
}

var (
	listening   = make(map[string]acceptChanConn)
	listeningMu sync.Mutex
)

// Connection is a channel based connection.
type Connection struct {
	cc chanConn
}

// Close ...
func (cc *Connection) Close() {
	close(cc.cc.senderClosed)
}

// SendMessageBatch ...
func (cc *Connection) SendMessageBatch(batch pb.MessageBatch) error {
	if cc.cc.snapshot {
		panic("sending message on snapshot cc")
	}
	data := pb.MustMarshal(&batch)
	select {
	case <-cc.cc.recverClosed:
		return ErrClosed
	case cc.cc.dataChan <- data:
	}
	return nil
}

// SnapshotConnection is a channel based snapshot connection.
type SnapshotConnection struct {
	cc chanConn
}

// Close ...
func (csc *SnapshotConnection) Close() {
	close(csc.cc.senderClosed)
}

// SendChunk ...
func (csc *SnapshotConnection) SendChunk(chunk pb.Chunk) error {
	if !csc.cc.snapshot {
		panic("sending snapshot data on regular cc")
	}
	data := pb.MustMarshal(&chunk)
	select {
	case <-csc.cc.recverClosed:
		return ErrClosed
	case csc.cc.dataChan <- data:
	}
	return nil
}

// Transport is a channel based transport module used for testing purposes.
type Transport struct {
	nhConfig       config.NodeHostConfig
	requestHandler raftio.MessageHandler
	chunkHandler   raftio.ChunkHandler
	stopper        *syncutil.Stopper
	connStopper    *syncutil.Stopper
}

// NewChanTransport creates a new channel based test transport module.
func NewChanTransport(nhConfig config.NodeHostConfig,
	requestHandler raftio.MessageHandler,
	chunkHandler raftio.ChunkHandler) *Transport {
	return &Transport{
		nhConfig:       nhConfig,
		requestHandler: requestHandler,
		chunkHandler:   chunkHandler,
		stopper:        syncutil.NewStopper(),
		connStopper:    syncutil.NewStopper(),
	}
}

// Start ...
func (ct *Transport) Start() error {
	acc := acceptChanConn{
		ac:  make(chan chanConn, 1),
		acc: make(chan struct{}),
	}
	func() {
		listeningMu.Lock()
		defer listeningMu.Unlock()
		listening[ct.nhConfig.RaftAddress] = acc
	}()
	ct.stopper.RunWorker(func() {
		for {
			select {
			case <-ct.stopper.ShouldStop():
				func() {
					listeningMu.Lock()
					defer listeningMu.Unlock()
					close(acc.acc)
					delete(listening, ct.nhConfig.RaftAddress)
				}()
				return
			case cc := <-acc.ac:
				ct.connStopper.RunWorker(func() {
					ct.serveConn(cc)
				})
			}
		}
	})
	return nil
}

// Close ...
func (ct *Transport) Close() error {
	ct.stopper.Stop()
	ct.connStopper.Stop()
	return nil
}

// Name ...
func (ct *Transport) Name() string {
	return "Transport"
}

func (ct *Transport) getConnection(target string,
	snapshot bool) (chanConn, error) {
	listeningMu.Lock()
	defer listeningMu.Unlock()
	acc, ok := listening[target]
	if !ok {
		return chanConn{}, ErrFailedToConnect
	}
	cc := createChanConn(snapshot)
	select {
	case <-acc.acc:
		return chanConn{}, ErrFailedToConnect
	case acc.ac <- cc:
	}
	return cc, nil
}

// GetConnection ...
func (ct *Transport) GetConnection(ctx context.Context,
	target string) (raftio.IConnection, error) {
	cc, err := ct.getConnection(target, false)
	if err != nil {
		return nil, err
	}
	return &Connection{cc: cc}, nil
}

// GetSnapshotConnection ...
func (ct *Transport) GetSnapshotConnection(ctx context.Context,
	target string) (raftio.ISnapshotConnection, error) {
	cc, err := ct.getConnection(target, true)
	if err != nil {
		return nil, err
	}
	return &SnapshotConnection{cc: cc}, nil
}

func (ct *Transport) process(data []byte, cc chanConn) bool {
	if cc.snapshot {
		chunk := pb.Chunk{}
		if err := chunk.Unmarshal(data); err != nil {
			return false
		}
		if !ct.chunkHandler(chunk) {
			return false
		}
	} else {
		batch := pb.MessageBatch{}
		if err := batch.Unmarshal(data); err != nil {
			return false
		}
		ct.requestHandler(batch)
	}
	return true
}

func (ct *Transport) serveConn(cc chanConn) {
	defer close(cc.recverClosed)
	done := false
	for !done {
		select {
		case data := <-cc.dataChan:
			if !ct.process(data, cc) {
				return
			}
		case <-ct.stopper.ShouldStop():
			return
		case <-cc.senderClosed:
			done = true
			break
		}
	}
	for {
		select {
		case data := <-cc.dataChan:
			if !ct.process(data, cc) {
				return
			}
		default:
			return
		}
	}
}

func createChanConn(snapshot bool) chanConn {
	return chanConn{
		snapshot:     snapshot,
		senderClosed: make(chan struct{}),
		recverClosed: make(chan struct{}),
		dataChan:     make(chan []byte, 8),
	}
}
