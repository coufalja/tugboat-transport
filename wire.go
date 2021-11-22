package transport

import (
	"context"

	"github.com/coufalja/tugboat/config"
	"github.com/coufalja/tugboat/raftio"
)

type WireTransport interface {
	// Name returns the type name of the ITransport instance.
	Name() string
	// Start launches the transport module and make it ready to start sending and
	// receiving Raft messages. If necessary, ITransport may take this opportunity
	// to start listening for incoming data.
	Start() error
	// Close closes the transport module.
	Close() error
	// GetConnection returns an IConnection instance used for sending messages
	// to the specified target NodeHost instance.
	GetConnection(ctx context.Context, target string) (raftio.IConnection, error)
	// GetSnapshotConnection returns an ISnapshotConnection instance used for
	// sending snapshot chunks to the specified target NodeHost instance.
	GetSnapshotConnection(ctx context.Context,
		target string) (raftio.ISnapshotConnection, error)
}

// WireTransportFunc is the interface used for creating custom transport modules.
type WireTransportFunc func(config.NodeHostConfig, raftio.MessageHandler, raftio.ChunkHandler) WireTransport
