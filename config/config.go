package config

import (
	"context"

	"github.com/coufalja/tugboat/raftio"
	"github.com/lni/vfs"
)

// IResolver converts the (cluster id, node id( tuple to network address.
type IResolver interface {
	Resolve(uint64, uint64) (string, string, error)
	Add(uint64, uint64, string)
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
	DeploymentID     uint64
	Resolver         IResolver
	FS               vfs.FS
	MaxSendQueueSize uint64

	// MaxSnapshotSendBytesPerSecond defines how much snapshot data can be sent
	// every second for all Raft clusters managed by the NodeHost instance.
	// The default value 0 means there is no limit set for snapshot streaming.
	MaxSnapshotSendBytesPerSecond uint64
	// MaxSnapshotRecvBytesPerSecond defines how much snapshot data can be
	// received each second for all Raft clusters managed by the NodeHost instance.
	// The default value 0 means there is no limit for receiving snapshot data.
	MaxSnapshotRecvBytesPerSecond uint64
	// MutualTLS defines whether to use mutual TLS for authenticating servers
	// and clients. Insecure communication is used when MutualTLS is set to
	// False.
	// See https://github.com/lni/dragonboat/wiki/TLS-in-Dragonboat for more
	// details on how to use Mutual TLS.
	MutualTLS bool
	// CAFile is the path of the CA certificate file. This field is ignored when
	// MutualTLS is false.
	CAFile string
	// CertFile is the path of the node certificate file. This field is ignored
	// when MutualTLS is false.
	CertFile string
	// KeyFile is the path of the node key file. This field is ignored when
	// MutualTLS is false.
	KeyFile string
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
	RaftAddress string
	// ListenAddress is an optional field in the hostname:port or IP:port address
	// form used by the transport module to listen on for Raft message and
	// snapshots. When the ListenAddress field is not set, The transport module
	// listens on RaftAddress. If 0.0.0.0 is specified as the IP of the
	// ListenAddress, Dragonboat listens to the specified port on all network
	// interfaces. When hostname or domain name is used, it will be resolved to
	// IPv4 addresses first and Dragonboat listens to all resolved IPv4 addresses.
	ListenAddress string
}
