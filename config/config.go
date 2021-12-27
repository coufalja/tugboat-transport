package config

import (
	"context"

	"github.com/coufalja/tugboat/raftio"
	pb "github.com/coufalja/tugboat/raftpb"
	"github.com/lni/vfs"
)

// IResolver converts the (cluster id, node id( tuple to network address.
type IResolver interface {
	Resolve(uint64, uint64) (string, string, error)
	Add(uint64, uint64, string)
}

// IMessageHandler is the interface required to handle incoming raft requests.
type IMessageHandler interface {
	HandleMessageBatch(batch pb.MessageBatch) (uint64, uint64)
	HandleUnreachable(clusterID uint64, nodeID uint64)
	HandleSnapshotStatus(clusterID uint64, nodeID uint64, rejected bool)
	HandleSnapshot(clusterID uint64, nodeID uint64, from uint64)
}

// ITransportEvent is the interface for notifying connection status changes.
type ITransportEvent interface {
	ConnectionEstablished(string, bool)
	ConnectionFailed(string, bool)
}

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
type WireTransportFunc func(string, uint64, raftio.MessageHandler, raftio.ChunkHandler) WireTransport

type Config struct {
	// RaftAddress is a DNS name:port or IP:port address used by the transport
	// module for exchanging Raft messages, snapshots and metadata between
	// NodeHost instances. It should be set to the public address that can be
	// accessed from remote NodeHost instances.
	//
	// When the NodeHostConfig.ListenAddress field is empty, NodeHost listens on
	// RaftAddress for incoming Raft messages. When hostname or domain name is
	// used, it will be resolved to IPv4 addresses first and Dragonboat listens
	// to all resolved IPv4 addresses.
	//
	// By default, the RaftAddress value is not allowed to change between NodeHost
	// restarts. AddressByNodeHostID should be set to true when the RaftAddress
	// value might change after restart.
	RaftAddress      string
	DeploymentID     uint64
	WireFactory      WireTransportFunc
	Resolver         IResolver
	SnapshotDir      func(clusterID uint64, nodeID uint64) string
	SysEvents        ITransportEvent
	FS               vfs.FS
	MaxSendQueueSize uint64
}
