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

package tcp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"time"

	"github.com/coufalja/tugboat/logger"
	"github.com/juju/ratelimit"
	"github.com/lni/goutils/netutil"
	"github.com/lni/goutils/syncutil"

	"github.com/coufalja/tugboat/raftio"
	pb "github.com/coufalja/tugboat/raftpb"
)

var (
	// ErrBadMessage is the error returned to indicate the incoming message is
	// corrupted.
	ErrBadMessage              = errors.New("invalid message")
	errPoisonReceived          = errors.New("poison received")
	magicNumber                = [2]byte{0xAE, 0x7D}
	poisonNumber               = [2]byte{0x0, 0x0}
	payloadBufferSize          = 2*1024*1024 + 1024*128
	tlsHandshackTimeout        = 10 * time.Second
	magicNumberDuration        = 1 * time.Second
	headerDuration             = 2 * time.Second
	readDuration               = 5 * time.Second
	writeDuration              = 5 * time.Second
	keepAlivePeriod            = 10 * time.Second
	perConnBufSize      uint64 = 2 * 1024 * 1024
	recvBufSize         uint64 = 2 * 1024 * 1024

	plog              = logger.GetLogger("transport")
	sendQueueLen      = 1024 * 2
	dialTimeoutSecond = 15
	idleTimeout       = time.Minute
)

const (
	// TCPTransportName is the name of the tcp transport module.
	TCPTransportName         = "go-tcp-transport"
	requestHeaderSize        = 18
	raftType          uint16 = 100
	snapshotType      uint16 = 200
)

type requestHeader struct {
	size   uint64
	crc    uint32
	method uint16
}

// TODO:
// Transport is never reliable [1]. dragonboat uses application layer crc32 checksum
// to help protecting raft state and log from some faulty network switches or
// buggy kernels. However, this is not necessary when TLS encryption is used.
// Update tcp.go to stop crc32 checking messages when TLS is used.
//
// [1] twitter's 2015 data corruption accident -
// https://www.evanjones.ca/checksum-failure-is-a-kernel-bug.html
// https://www.evanjones.ca/tcp-and-ethernet-checksums-fail.html
func (h *requestHeader) encode(buf []byte) []byte {
	if len(buf) < requestHeaderSize {
		panic("input buf too small")
	}
	binary.BigEndian.PutUint16(buf, h.method)
	binary.BigEndian.PutUint64(buf[2:], h.size)
	binary.BigEndian.PutUint32(buf[10:], 0)
	binary.BigEndian.PutUint32(buf[14:], h.crc)
	v := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	binary.BigEndian.PutUint32(buf[10:], v)
	return buf[:requestHeaderSize]
}

func (h *requestHeader) decode(buf []byte) bool {
	if len(buf) < requestHeaderSize {
		return false
	}
	incoming := binary.BigEndian.Uint32(buf[10:])
	binary.BigEndian.PutUint32(buf[10:], 0)
	expected := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	if incoming != expected {
		plog.Errorf("header crc check failed")
		return false
	}
	binary.BigEndian.PutUint32(buf[10:], incoming)
	method := binary.BigEndian.Uint16(buf)
	if method != raftType && method != snapshotType {
		plog.Errorf("invalid method type")
		return false
	}
	h.method = method
	h.size = binary.BigEndian.Uint64(buf[2:])
	h.crc = binary.BigEndian.Uint32(buf[14:])
	return true
}

func sendPoison(conn net.Conn, poison []byte) error {
	tt := time.Now().Add(magicNumberDuration).Add(magicNumberDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(poison); err != nil {
		return err
	}
	return nil
}

func sendPoisonAck(conn net.Conn, poisonAck []byte) error {
	return sendPoison(conn, poisonAck)
}

func waitPoisonAck(conn net.Conn) {
	ack := make([]byte, len(poisonNumber))
	tt := time.Now().Add(keepAlivePeriod)
	if err := conn.SetReadDeadline(tt); err != nil {
		return
	}
	if _, err := io.ReadFull(conn, ack); err != nil {
		plog.Errorf("failed to get poison ack %v", err)
		return
	}
}

func writeMessage(conn net.Conn,
	header requestHeader, buf []byte, headerBuf []byte, encrypted bool) error {
	header.size = uint64(len(buf))
	if !encrypted {
		header.crc = crc32.ChecksumIEEE(buf)
	}
	headerBuf = header.encode(headerBuf)
	tt := time.Now().Add(magicNumberDuration).Add(headerDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(magicNumber[:]); err != nil {
		return err
	}
	if _, err := conn.Write(headerBuf); err != nil {
		return err
	}
	sent := 0
	bufSize := int(recvBufSize)
	for sent < len(buf) {
		if sent+bufSize > len(buf) {
			bufSize = len(buf) - sent
		}
		tt = time.Now().Add(writeDuration)
		if err := conn.SetWriteDeadline(tt); err != nil {
			return err
		}
		if _, err := conn.Write(buf[sent : sent+bufSize]); err != nil {
			return err
		}
		sent += bufSize
	}
	if sent != len(buf) {
		plog.Panicf("sent %d, buf len %d", sent, len(buf))
	}
	return nil
}

func readMessage(conn net.Conn,
	header []byte, rbuf []byte, encrypted bool) (requestHeader, []byte, error) {
	tt := time.Now().Add(headerDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return requestHeader{}, nil, err
	}
	if _, err := io.ReadFull(conn, header); err != nil {
		plog.Errorf("failed to get the header")
		return requestHeader{}, nil, err
	}
	rheader := requestHeader{}
	if !rheader.decode(header) {
		plog.Errorf("invalid header")
		return requestHeader{}, nil, ErrBadMessage
	}
	if rheader.size == 0 {
		plog.Errorf("invalid payload length")
		return requestHeader{}, nil, ErrBadMessage
	}
	var buf []byte
	if rheader.size > uint64(len(rbuf)) {
		buf = make([]byte, rheader.size)
	} else {
		buf = rbuf[:rheader.size]
	}
	received := uint64(0)
	var recvBuf []byte
	if rheader.size < recvBufSize {
		recvBuf = buf[:rheader.size]
	} else {
		recvBuf = buf[:recvBufSize]
	}
	toRead := rheader.size
	for toRead > 0 {
		tt = time.Now().Add(readDuration)
		if err := conn.SetReadDeadline(tt); err != nil {
			return requestHeader{}, nil, err
		}
		if _, err := io.ReadFull(conn, recvBuf); err != nil {
			return requestHeader{}, nil, err
		}
		toRead -= uint64(len(recvBuf))
		received += uint64(len(recvBuf))
		if toRead < recvBufSize {
			recvBuf = buf[received : received+toRead]
		} else {
			recvBuf = buf[received : received+recvBufSize]
		}
	}
	if received != rheader.size {
		panic("unexpected size")
	}
	if !encrypted && crc32.ChecksumIEEE(buf) != rheader.crc {
		plog.Errorf("invalid payload checksum")
		return requestHeader{}, nil, ErrBadMessage
	}
	return rheader, buf, nil
}

func readMagicNumber(conn net.Conn, magicNum []byte) error {
	tt := time.Now().Add(magicNumberDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return err
	}
	if _, err := io.ReadFull(conn, magicNum); err != nil {
		return err
	}
	if bytes.Equal(magicNum, poisonNumber[:]) {
		return errPoisonReceived
	}
	if !bytes.Equal(magicNum, magicNumber[:]) {
		return ErrBadMessage
	}
	return nil
}

type connection struct {
	conn net.Conn
	lr   io.Reader
	lw   io.Writer
}

func newConnection(conn net.Conn,
	rb *ratelimit.Bucket, wb *ratelimit.Bucket) net.Conn {
	c := &connection{conn: conn}
	if rb != nil {
		c.lr = ratelimit.Reader(conn, rb)
	}
	if wb != nil {
		c.lw = ratelimit.Writer(conn, wb)
	}
	return c
}

func (c *connection) Close() error {
	return c.conn.Close()
}

func (c *connection) Read(b []byte) (int, error) {
	if c.lr != nil {
		return c.lr.Read(b)
	}
	return c.conn.Read(b)
}

func (c *connection) Write(b []byte) (int, error) {
	if c.lw != nil {
		return c.lw.Write(b)
	}
	return c.conn.Write(b)
}

func (c *connection) LocalAddr() net.Addr {
	panic("not implemented")
}

func (c *connection) RemoteAddr() net.Addr {
	panic("not implemented")
}

func (c *connection) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *connection) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *connection) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// Connection is the connection used for sending raft messages to remote
// nodes.
type Connection struct {
	conn      net.Conn
	header    []byte
	payload   []byte
	encrypted bool
}

var _ raftio.IConnection = (*Connection)(nil)

// NewConnection creates and returns a new Connection instance.
func NewConnection(conn net.Conn,
	rb *ratelimit.Bucket, wb *ratelimit.Bucket, encrypted bool) *Connection {
	return &Connection{
		conn:      newConnection(conn, rb, wb),
		header:    make([]byte, requestHeaderSize),
		payload:   make([]byte, perConnBufSize),
		encrypted: encrypted,
	}
}

// Close closes the Connection instance.
func (c *Connection) Close() {
	if err := c.conn.Close(); err != nil {
		plog.Errorf("failed to close the connection %v", err)
	}
}

// SendMessageBatch sends a raft message batch to remote node.
func (c *Connection) SendMessageBatch(batch pb.MessageBatch) error {
	header := requestHeader{method: raftType}
	sz := batch.SizeUpperLimit()
	var buf []byte
	if len(c.payload) < sz {
		buf = make([]byte, sz)
	} else {
		buf = c.payload
	}
	buf = pb.MustMarshalTo(&batch, buf)
	return writeMessage(c.conn, header, buf, c.header, c.encrypted)
}

// SnapshotConnection is the connection for sending raft snapshot chunks to
// remote nodes.
type SnapshotConnection struct {
	conn      net.Conn
	header    []byte
	encrypted bool
}

var _ raftio.ISnapshotConnection = (*SnapshotConnection)(nil)

// NewSnapshotConnection creates and returns a new snapshot connection.
func NewSnapshotConnection(conn net.Conn,
	rb *ratelimit.Bucket, wb *ratelimit.Bucket,
	encrypted bool) *SnapshotConnection {
	return &SnapshotConnection{
		conn:      newConnection(conn, rb, wb),
		header:    make([]byte, requestHeaderSize),
		encrypted: encrypted,
	}
}

// Close closes the snapshot connection.
func (c *SnapshotConnection) Close() {
	defer func() {
		if err := c.conn.Close(); err != nil {
			plog.Debugf("failed to close the connection %v", err)
		}
	}()
	if err := sendPoison(c.conn, poisonNumber[:]); err != nil {
		return
	}
	waitPoisonAck(c.conn)
}

// SendChunk sends the specified snapshot chunk to remote node.
func (c *SnapshotConnection) SendChunk(chunk pb.Chunk) error {
	header := requestHeader{method: snapshotType}
	sz := chunk.Size()
	buf := make([]byte, sz)
	buf = pb.MustMarshalTo(&chunk, buf)
	return writeMessage(c.conn, header, buf, c.header, c.encrypted)
}

// Transport is a Transport based transport module for exchanging raft messages and
// snapshots between NodeHost instances.
type Transport struct {
	readBucket     *ratelimit.Bucket
	stopper        *syncutil.Stopper
	connStopper    *syncutil.Stopper
	requestHandler raftio.MessageHandler
	chunkHandler   raftio.ChunkHandler
	writeBucket    *ratelimit.Bucket
	encrypted      bool
	cfg            Config
}

type Config struct {
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

// GetListenAddress returns the actual address the transport module is going to
// listen on.
func (c *Config) GetListenAddress() string {
	if len(c.ListenAddress) > 0 {
		return c.ListenAddress
	}
	return c.RaftAddress
}

// GetServerTLSConfig returns the server tls.Config instance based on the
// TLS settings in NodeHostConfig.
func (c *Config) GetServerTLSConfig() (*tls.Config, error) {
	if c.MutualTLS {
		return netutil.GetServerTLSConfig(c.CAFile, c.CertFile, c.KeyFile)
	}
	return nil, nil
}

// GetClientTLSConfig returns the client tls.Config instance for the specified
// target based on the TLS settings in NodeHostConfig.
func (c *Config) GetClientTLSConfig(target string) (*tls.Config, error) {
	if c.MutualTLS {
		tlsConfig, err := netutil.GetClientTLSConfig("",
			c.CAFile, c.CertFile, c.KeyFile)
		if err != nil {
			return nil, err
		}
		host, err := netutil.GetHost(target)
		if err != nil {
			return nil, err
		}
		return &tls.Config{
			ServerName:   host,
			Certificates: tlsConfig.Certificates,
			RootCAs:      tlsConfig.RootCAs,
		}, nil
	}
	return nil, nil
}

// Factory is a convenience Factory method
func Factory(cfg Config) func(messageHandler raftio.MessageHandler, chunkHandler raftio.ChunkHandler) *Transport {
	return func(messageHandler raftio.MessageHandler, chunkHandler raftio.ChunkHandler) *Transport {
		return New(cfg, messageHandler, chunkHandler)
	}
}

// New creates and returns a new transport module.
func New(cfg Config, requestHandler raftio.MessageHandler, chunkHandler raftio.ChunkHandler) *Transport {
	t := &Transport{
		cfg:            cfg,
		stopper:        syncutil.NewStopper(),
		connStopper:    syncutil.NewStopper(),
		requestHandler: requestHandler,
		chunkHandler:   chunkHandler,
		encrypted:      cfg.MutualTLS,
	}
	rate := cfg.MaxSnapshotSendBytesPerSecond
	if rate > 0 {
		t.writeBucket = ratelimit.NewBucketWithRate(float64(rate), int64(rate)*2)
	}
	rate = cfg.MaxSnapshotRecvBytesPerSecond
	if rate > 0 {
		t.readBucket = ratelimit.NewBucketWithRate(float64(rate), int64(rate)*2)
	}
	return t
}

// Start starts the Transport transport module.
func (t *Transport) Start() error {
	address := t.cfg.GetListenAddress()
	tlsConfig, err := t.cfg.GetServerTLSConfig()
	if err != nil {
		return err
	}
	listener, err := netutil.NewStoppableListener(address,
		tlsConfig, t.stopper.ShouldStop())
	if err != nil {
		return err
	}
	t.connStopper.RunWorker(func() {
		// sync.WaitGroup's doc mentions that
		// "Note that calls with a positive delta that occur when the counter is
		//  zero must happen before a Wait."
		// It is unclear that whether the stdlib is going complain in future
		// releases when Wait() is called when the counter is zero and Add() with
		// positive delta has never been called.
		<-t.connStopper.ShouldStop()
	})
	t.stopper.RunWorker(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if err == netutil.ErrListenerStopped {
					return
				}
				panic(err)
			}
			var once sync.Once
			closeFn := func() {
				once.Do(func() {
					if err := conn.Close(); err != nil {
						plog.Errorf("failed to close the connection %v", err)
					}
				})
			}
			t.connStopper.RunWorker(func() {
				<-t.stopper.ShouldStop()
				closeFn()
			})
			t.connStopper.RunWorker(func() {
				t.serveConn(conn)
				closeFn()
			})
		}
	})
	return nil
}

// Close closes the Transport transport module.
func (t *Transport) Close() error {
	t.stopper.Stop()
	t.connStopper.Stop()
	return nil
}

// GetConnection returns a new raftio.IConnection for sending raft messages.
func (t *Transport) GetConnection(ctx context.Context,
	target string) (raftio.IConnection, error) {
	conn, err := t.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewConnection(conn, nil, nil, t.encrypted), nil
}

// GetSnapshotConnection returns a new raftio.IConnection for sending raft
// snapshots.
func (t *Transport) GetSnapshotConnection(ctx context.Context,
	target string) (raftio.ISnapshotConnection, error) {
	conn, err := t.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewSnapshotConnection(conn,
		t.readBucket, t.writeBucket, t.encrypted), nil
}

// Name returns a human readable name of the Transport transport module.
func (t *Transport) Name() string {
	return TCPTransportName
}

func (t *Transport) serveConn(conn net.Conn) {
	magicNum := make([]byte, len(magicNumber))
	header := make([]byte, requestHeaderSize)
	tbuf := make([]byte, payloadBufferSize)
	for {
		err := readMagicNumber(conn, magicNum)
		if err != nil {
			if errors.Is(err, errPoisonReceived) {
				if err := sendPoisonAck(conn, poisonNumber[:]); err != nil {
					plog.Debugf("failed to send poison ack %v", err)
				}
				return
			}
			if errors.Is(err, ErrBadMessage) {
				return
			}
			operr, ok := err.(net.Error)
			if ok && operr.Timeout() {
				continue
			} else {
				return
			}
		}
		rheader, buf, err := readMessage(conn, header, tbuf, t.encrypted)
		if err != nil {
			return
		}
		if rheader.method == raftType {
			batch := pb.MessageBatch{}
			if err := batch.Unmarshal(buf); err != nil {
				return
			}
			t.requestHandler(batch)
		} else {
			chunk := pb.Chunk{}
			if err := chunk.Unmarshal(buf); err != nil {
				return
			}
			if !t.chunkHandler(chunk) {
				plog.Errorf("chunk rejected %s", chunkKey(chunk))
				return
			}
		}
	}
}

func chunkKey(c pb.Chunk) string {
	return fmt.Sprintf("%d:%d:%d", c.ClusterId, c.NodeId, c.Index)
}

func setTCPConn(conn *net.TCPConn) error {
	if err := conn.SetLinger(0); err != nil {
		return err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		return err
	}
	return conn.SetKeepAlivePeriod(keepAlivePeriod)
}

// FIXME:
// context.Context is ignored
func (t *Transport) getConnection(ctx context.Context,
	target string) (net.Conn, error) {
	timeout := time.Duration(dialTimeoutSecond) * time.Second
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return nil, err
	}
	tcpconn, ok := conn.(*net.TCPConn)
	if ok {
		if err := setTCPConn(tcpconn); err != nil {
			return nil, err
		}
	}
	tlsConfig, err := t.cfg.GetClientTLSConfig(target)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		conn = tls.Client(conn, tlsConfig)
		tt := time.Now().Add(tlsHandshackTimeout)
		if err := conn.SetDeadline(tt); err != nil {
			return nil, err
		}
		if err := conn.(*tls.Conn).Handshake(); err != nil {
			return nil, err
		}
	}
	return conn, nil
}
