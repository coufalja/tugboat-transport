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

package integration

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coufalja/tugboat-transport/noop"
	"github.com/coufalja/tugboat-transport/tcp"
	"github.com/coufalja/tugboat/config"
	"github.com/coufalja/tugboat/raftio"
	"github.com/coufalja/tugboat/raftpb"
	"github.com/coufalja/tugboat/rsm"
	"github.com/coufalja/tugboat/transport"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/goutils/netutil"
	"github.com/lni/goutils/syncutil"
	"github.com/lni/vfs"
)

var serverAddress = fmt.Sprintf("localhost:%d", getTestPort())

const (
	snapshotDir       = "gtransport_test_data_safe_to_delete"
	caFile            = "tests/test-root-ca.crt"
	certFile          = "tests/localhost.crt"
	keyFile           = "tests/localhost.key"
	testSnapshotIndex = uint64(12345)
)

const defaultTestPort = 26001

func getTestPort() int {
	pv := os.Getenv("DRAGONBOAT_TEST_PORT")
	if len(pv) > 0 {
		port, err := strconv.Atoi(pv)
		if err != nil {
			panic(err)
		}
		return port
	}
	return defaultTestPort
}

type dummyTransportEvent struct{}

func (d *dummyTransportEvent) ConnectionEstablished(addr string, snapshot bool) {}
func (d *dummyTransportEvent) ConnectionFailed(addr string, snapshot bool)      {}

type testSnapshotDir struct {
	fs vfs.FS
}

func newTestSnapshotDir(fs vfs.FS) *testSnapshotDir {
	return &testSnapshotDir{fs: fs}
}

func (g *testSnapshotDir) GetSnapshotRootDir(clusterID uint64,
	nodeID uint64) string {
	snapNodeDir := fmt.Sprintf("snapshot-%d-%d", clusterID, nodeID)
	return g.fs.PathJoin(snapshotDir, snapNodeDir)
}

func (g *testSnapshotDir) GetSnapshotDir(clusterID uint64,
	nodeID uint64, lastApplied uint64) string {
	snapNodeDir := fmt.Sprintf("snapshot-%d-%d", clusterID, nodeID)
	snapDir := fmt.Sprintf("snapshot-%016X", lastApplied)
	d := g.fs.PathJoin(snapshotDir, snapNodeDir, snapDir)
	return d
}

func (g *testSnapshotDir) getSnapshotFileMD5(clusterID uint64,
	nodeID uint64, index uint64, filename string) ([]byte, error) {
	snapDir := g.GetSnapshotDir(clusterID, nodeID, index)
	fp := g.fs.PathJoin(snapDir, filename)
	f, err := g.fs.Open(fp)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func (g *testSnapshotDir) generateSnapshotExternalFile(clusterID uint64,
	nodeID uint64, index uint64, filename string, sz uint64) {
	snapDir := g.GetSnapshotDir(clusterID, nodeID, index)
	if err := g.fs.MkdirAll(snapDir, 0o755); err != nil {
		panic(err)
	}
	fp := g.fs.PathJoin(snapDir, filename)
	data := make([]byte, sz)
	rand.Read(data)
	f, err := g.fs.Create(fp)
	if err != nil {
		panic(err)
	}
	n, err := f.Write(data)
	if n != len(data) {
		panic("failed to write all files")
	}
	if err != nil {
		panic(err)
	}
	f.Close()
}

func (g *testSnapshotDir) generateSnapshotFile(clusterID uint64,
	nodeID uint64, index uint64, filename string, sz uint64, fs vfs.FS) {
	snapDir := g.GetSnapshotDir(clusterID, nodeID, index)
	if err := g.fs.MkdirAll(snapDir, 0o755); err != nil {
		panic(err)
	}
	fp := g.fs.PathJoin(snapDir, filename)
	data := make([]byte, sz)
	rand.Read(data)
	writer, err := rsm.NewSnapshotWriter(fp, raftpb.NoCompression, fs)
	if err != nil {
		panic(err)
	}
	n, err := writer.Write(data)
	if n != len(data) {
		panic("short write")
	}
	if err != nil {
		panic(err)
	}
	/*n, err = writer.Write(data)
	if n != len(data) {
		panic("short write")
	}
	if err != nil {
		panic(err)
	}*/
	if err := writer.Close(); err != nil {
		panic(err)
	}
}

func (g *testSnapshotDir) cleanup() {
	if err := g.fs.RemoveAll(snapshotDir); err != nil {
		panic(err)
	}
}

type testMessageHandler struct {
	mu                        sync.Mutex
	requestCount              map[raftio.NodeInfo]uint64
	unreachableCount          map[raftio.NodeInfo]uint64
	snapshotCount             map[raftio.NodeInfo]uint64
	snapshotFailedCount       map[raftio.NodeInfo]uint64
	snapshotSuccessCount      map[raftio.NodeInfo]uint64
	receivedSnapshotCount     map[raftio.NodeInfo]uint64
	receivedSnapshotFromCount map[raftio.NodeInfo]uint64
}

func newTestMessageHandler() *testMessageHandler {
	return &testMessageHandler{
		requestCount:              make(map[raftio.NodeInfo]uint64),
		unreachableCount:          make(map[raftio.NodeInfo]uint64),
		snapshotCount:             make(map[raftio.NodeInfo]uint64),
		snapshotFailedCount:       make(map[raftio.NodeInfo]uint64),
		snapshotSuccessCount:      make(map[raftio.NodeInfo]uint64),
		receivedSnapshotCount:     make(map[raftio.NodeInfo]uint64),
		receivedSnapshotFromCount: make(map[raftio.NodeInfo]uint64),
	}
}

func (h *testMessageHandler) HandleMessageBatch(reqs raftpb.MessageBatch) (uint64, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ss := uint64(0)
	msg := uint64(0)
	for _, req := range reqs.Requests {
		epk := raftio.GetNodeInfo(req.ClusterId, req.To)
		v, ok := h.requestCount[epk]
		if ok {
			h.requestCount[epk] = v + 1
		} else {
			h.requestCount[epk] = 1
		}
		if req.Type == raftpb.InstallSnapshot {
			ss++
			v, ok = h.snapshotCount[epk]
			if ok {
				h.snapshotCount[epk] = v + 1
			} else {
				h.snapshotCount[epk] = 1
			}
		} else {
			msg++
		}
	}
	return ss, msg
}

func (h *testMessageHandler) HandleSnapshotStatus(clusterID uint64,
	nodeID uint64, failed bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	epk := raftio.GetNodeInfo(clusterID, nodeID)
	var p *map[raftio.NodeInfo]uint64
	if failed {
		p = &h.snapshotFailedCount
	} else {
		p = &h.snapshotSuccessCount
	}
	v, ok := (*p)[epk]
	if ok {
		(*p)[epk] = v + 1
	} else {
		(*p)[epk] = 1
	}
}

func (h *testMessageHandler) HandleUnreachable(clusterID uint64,
	nodeID uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	epk := raftio.GetNodeInfo(clusterID, nodeID)
	v, ok := h.unreachableCount[epk]
	if ok {
		h.unreachableCount[epk] = v + 1
	} else {
		h.unreachableCount[epk] = 1
	}
}

func (h *testMessageHandler) HandleSnapshot(clusterID uint64,
	nodeID uint64, from uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	epk := raftio.GetNodeInfo(clusterID, nodeID)
	v, ok := h.receivedSnapshotCount[epk]
	if ok {
		h.receivedSnapshotCount[epk] = v + 1
	} else {
		h.receivedSnapshotCount[epk] = 1
	}
	epk.NodeID = from
	v, ok = h.receivedSnapshotFromCount[epk]
	if ok {
		h.receivedSnapshotFromCount[epk] = v + 1
	} else {
		h.receivedSnapshotFromCount[epk] = 1
	}
}

func (h *testMessageHandler) getReceivedSnapshotCount(clusterID uint64,
	nodeID uint64) uint64 {
	return h.getMessageCount(h.receivedSnapshotCount, clusterID, nodeID)
}

func (h *testMessageHandler) getReceivedSnapshotFromCount(clusterID uint64,
	nodeID uint64) uint64 {
	return h.getMessageCount(h.receivedSnapshotFromCount, clusterID, nodeID)
}

func (h *testMessageHandler) getRequestCount(clusterID uint64,
	nodeID uint64) uint64 {
	return h.getMessageCount(h.requestCount, clusterID, nodeID)
}

func (h *testMessageHandler) getFailedSnapshotCount(clusterID uint64,
	nodeID uint64) uint64 {
	return h.getMessageCount(h.snapshotFailedCount, clusterID, nodeID)
}

func (h *testMessageHandler) getSnapshotSuccessCount(clusterID uint64,
	nodeID uint64) uint64 {
	return h.getMessageCount(h.snapshotSuccessCount, clusterID, nodeID)
}

func (h *testMessageHandler) getSnapshotCount(clusterID uint64,
	nodeID uint64) uint64 {
	return h.getMessageCount(h.snapshotCount, clusterID, nodeID)
}

func (h *testMessageHandler) getMessageCount(m map[raftio.NodeInfo]uint64,
	clusterID uint64, nodeID uint64) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	epk := raftio.GetNodeInfo(clusterID, nodeID)
	v, ok := m[epk]
	if ok {
		return v
	}
	return 0
}

func newNOOPTestTransport(handler raftpb.IMessageHandler, fs vfs.FS) (*transport.Transport[*noop.Transport], *testResolver, *noop.Transport, *noop.Request, *noop.ConnectRequest) {
	t := newTestSnapshotDir(fs)
	resolver := &testResolver{}
	c := config.NodeHostConfig{
		RaftAddress:      serverAddress,
		MaxSendQueueSize: 256 * 1024 * 1024,
	}
	c.Prepare()
	c.Expert.FS = fs
	tr, err := transport.Factory(c, resolver, handler, &dummyTransportEvent{}, t.GetSnapshotRootDir, noop.Factory())
	if err != nil {
		panic(err)
	}
	trans := tr.GetTrans()
	return tr, resolver, trans, trans.Req, trans.ConnReq
}

type testResolver struct {
	Addr sync.Map
}

func (t *testResolver) Resolve(clusterID uint64, nodeID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	addr, ok := t.Addr.Load(key)
	if !ok {
		return "", "", errors.New("node not found")
	}
	return addr.(string), addr.(string), nil
}

func (t *testResolver) Add(clusterID uint64, nodeID uint64, target string) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	_, _ = t.Addr.LoadOrStore(key, target)
}

func newTestTransport(handler raftpb.IMessageHandler, mutualTLS bool, fs vfs.FS) (*transport.Transport[*tcp.Transport], *testResolver, *syncutil.Stopper, *testSnapshotDir) {
	stopper := syncutil.NewStopper()
	resolver := &testResolver{}
	t := newTestSnapshotDir(fs)
	c := config.NodeHostConfig{
		RaftAddress:      serverAddress,
		MaxSendQueueSize: 256 * 1024 * 1024,
	}
	c.Prepare()
	c.Expert.FS = fs
	tr, err := transport.Factory(c, resolver, handler, &dummyTransportEvent{}, t.GetSnapshotRootDir, tcp.Factory(tcp.Config{
		RaftAddress:                   serverAddress,
		MaxSnapshotSendBytesPerSecond: 256 * 1024 * 1024,
		MaxSnapshotRecvBytesPerSecond: 256 * 1024 * 1024,
		MutualTLS:                     mutualTLS,
		CAFile:                        caFile,
		CertFile:                      certFile,
		KeyFile:                       keyFile,
	}))
	if err != nil {
		panic(err)
	}
	return tr, resolver, stopper, t
}

// add some latency to localhost
// sudo tc qdisc add dev lo root handle 1:0 netem delay 100msec
// remove latency
// sudo tc qdisc del dev lo root
// don't forget to change your Transport window size if necessary
// e.g. in our dev environment, we have -
// net.core.wmem_max = 25165824
// net.core.rmem_max = 25165824
// net.ipv4.tcp_rmem = 4096 87380 25165824
// net.ipv4.tcp_wmem = 4096 87380 25165824

func testMessageCanBeSentWithLargeLatency(t *testing.T, mutualTLS bool, fs vfs.FS) {
	handler := newTestMessageHandler()
	trans, nodes, stopper, _ := newTestTransport(handler, mutualTLS, fs)
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	nodes.Add(100, 2, serverAddress)
	for i := 0; i < 128; i++ {
		msg := raftpb.Message{
			Type:      raftpb.Replicate,
			To:        2,
			ClusterId: 100,
			Entries:   []raftpb.Entry{{Cmd: make([]byte, 1024)}},
		}
		done := trans.Send(msg)
		if !done {
			t.Errorf("failed to send message")
		}
	}
	done := false
	for i := 0; i < 400; i++ {
		time.Sleep(100 * time.Millisecond)
		if handler.getRequestCount(100, 2) == 128 {
			done = true
			break
		}
	}
	if !done {
		t.Errorf("failed to send/receive all messages")
	}
}

// latency need to be simulated by configuring your environment.
func TestMessageCanBeSentWithLargeLatency(t *testing.T) {
	fs := vfs.NewStrictMem()
	defer leaktest.AfterTest(t)()
	testMessageCanBeSentWithLargeLatency(t, true, fs)
	testMessageCanBeSentWithLargeLatency(t, false, fs)
}

func testMessageBatchWithNotMatchedDBVAreDropped(t *testing.T, f transport.SendMessageBatchFunc, mutualTLS bool, fs vfs.FS) {
	handler := newTestMessageHandler()
	trans, nodes, stopper, _ := newTestTransport(handler, mutualTLS, fs)
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	nodes.Add(100, 2, serverAddress)
	trans.SetPreSendBatchHook(f)
	for i := 0; i < 100; i++ {
		msg := raftpb.Message{
			Type:      raftpb.Heartbeat,
			To:        2,
			ClusterId: 100,
		}
		// silently drop
		done := trans.Send(msg)
		if !done {
			t.Errorf("failed to send message")
		}
	}
	time.Sleep(100 * time.Millisecond)
	if handler.getRequestCount(100, 2) != 0 {
		t.Errorf("got %d, want %d", handler.getRequestCount(100, 2), 0)
	}
}

func TestMessageBatchWithNotMatchedDeploymentIDAreDropped(t *testing.T) {
	fs := vfs.NewStrictMem()
	defer leaktest.AfterTest(t)()
	f := func(b raftpb.MessageBatch) (raftpb.MessageBatch, bool) {
		b.DeploymentId = 2
		return b, true
	}
	testMessageBatchWithNotMatchedDBVAreDropped(t, f, true, fs)
	testMessageBatchWithNotMatchedDBVAreDropped(t, f, false, fs)
}

func TestMessageBatchWithNotMatchedBinVerAreDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := func(b raftpb.MessageBatch) (raftpb.MessageBatch, bool) {
		b.BinVer = raftio.TransportBinVersion + 1
		return b, true
	}
	fs := vfs.NewStrictMem()
	testMessageBatchWithNotMatchedDBVAreDropped(t, f, true, fs)
	testMessageBatchWithNotMatchedDBVAreDropped(t, f, false, fs)
}

func TestCircuitBreaker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	breaker := netutil.NewBreaker()
	breaker.Fail()
	if breaker.Ready() {
		t.Errorf("breaker is still ready?")
	}
}

func TestCircuitBreakerKicksInOnConnectivityIssue(t *testing.T) {
	fs := vfs.NewStrictMem()
	defer leaktest.AfterTest(t)()
	handler := newTestMessageHandler()
	trans, nodes, stopper, _ := newTestTransport(handler, false, fs)
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	nodes.Add(100, 2, "nosuchhost:39001")
	msg := raftpb.Message{
		Type:      raftpb.Heartbeat,
		To:        2,
		From:      1,
		ClusterId: 100,
	}
	done := trans.Send(msg)
	if !done {
		t.Errorf("not suppose to fail")
	}
	for {
		if trans.QueueSize() == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	breaker := trans.GetCircuitBreaker("nosuchhost:39001")
	if breaker.Ready() {
		t.Errorf("breaker is still ready?")
	}
	time.Sleep(time.Second)
	if !breaker.Ready() {
		t.Errorf("breaker is not ready after wait")
	}
}

func getTestSnapshotMessage(to uint64) raftpb.Message {
	m := raftpb.Message{
		Type:      raftpb.InstallSnapshot,
		From:      12,
		To:        to,
		ClusterId: 100,
		Snapshot: raftpb.Snapshot{
			Membership: raftpb.Membership{
				ConfigChangeId: 178,
			},
			Index: testSnapshotIndex,
			Term:  19,
		},
	}
	m.Snapshot.Load(&noopCompactor{})
	return m
}

func TestSnapshotCanBeSent(t *testing.T) {
	fs := vfs.NewStrictMem()
	defer leaktest.AfterTest(t)()
	mutualTLSValues := []bool{true, false}
	for _, v := range mutualTLSValues {
		testSnapshotCanBeSent(t, transport.SnapshotChunkSize-1, 10000, v, fs)
		testSnapshotCanBeSent(t, transport.SnapshotChunkSize/2, 10000, v, fs)
		testSnapshotCanBeSent(t, transport.SnapshotChunkSize+1, 10000, v, fs)
		testSnapshotCanBeSent(t, transport.SnapshotChunkSize*3, 10000, v, fs)
		testSnapshotCanBeSent(t, transport.SnapshotChunkSize*3+1, 10000, v, fs)
		testSnapshotCanBeSent(t, transport.SnapshotChunkSize*3-1, 10000, v, fs)
	}
}

func testSourceAddressWillBeAddedToNodeRegistry(t *testing.T, mutualTLS bool, fs vfs.FS) {
	handler := newTestMessageHandler()
	trans, nodes, stopper, _ := newTestTransport(handler, mutualTLS, fs)
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	nodes.Add(100, 2, serverAddress)
	msg := raftpb.Message{
		Type:      raftpb.Heartbeat,
		To:        2,
		From:      200,
		ClusterId: 100,
	}
	done := trans.Send(msg)
	if !done {
		t.Errorf("not suppose to fail")
	}
	count := 0
	for count < 200 && handler.getRequestCount(100, 2) == 0 {
		count++
		time.Sleep(5 * time.Millisecond)
	}
	if count == 200 {
		t.Errorf("failed to send the message")
	}
	vc := 0
	nodes.Addr.Range(func(k, v interface{}) bool {
		vc++
		return true
	})
	if vc != 2 {
		t.Errorf("remote address not updated")
	}
	key := raftio.GetNodeInfo(100, 200)
	v, ok := nodes.Addr.Load(key)
	if !ok {
		t.Errorf("deploymentID not record source address")
	}
	if v.(string) != serverAddress {
		t.Errorf("v %s, want %s", v, serverAddress)
	}
}

func TestSourceAddressWillBeAddedToNodeRegistry(t *testing.T) {
	fs := vfs.NewStrictMem()
	defer leaktest.AfterTest(t)()
	testSourceAddressWillBeAddedToNodeRegistry(t, true, fs)
	testSourceAddressWillBeAddedToNodeRegistry(t, false, fs)
}

func waitForTotalSnapshotStatusUpdateCount(handler *testMessageHandler,
	maxWait uint64, count uint64) {
	total := uint64(0)

	for total < maxWait {
		time.Sleep(10 * time.Millisecond)
		total += 10
		handler.mu.Lock()
		c := uint64(0)
		for _, v := range handler.snapshotFailedCount {
			c += v
		}
		for _, v := range handler.snapshotSuccessCount {
			c += v
		}
		handler.mu.Unlock()

		if c >= count {
			return
		}
	}
}

func waitForFirstSnapshotStatusUpdate(handler *testMessageHandler,
	maxWait uint64) {
	total := uint64(0)
	for total < maxWait {
		time.Sleep(10 * time.Millisecond)
		total += 10
		if handler.getFailedSnapshotCount(100, 2) > 0 ||
			handler.getSnapshotSuccessCount(100, 2) > 0 {
			return
		}
	}
}

func waitForSnapshotCountUpdate(handler *testMessageHandler, maxWait uint64) {
	total := uint64(0)
	for total < maxWait {
		time.Sleep(10 * time.Millisecond)
		total += 10
		count := handler.getReceivedSnapshotCount(100, 2)
		if count > 0 {
			return
		}
	}
}

func getTestSnapshotFileSize(sz uint64) uint64 {
	if rsm.DefaultVersion == rsm.V1 {
		return sz + rsm.HeaderSize
	} else if rsm.DefaultVersion == rsm.V2 {
		return rsm.GetV2PayloadSize(sz) + rsm.HeaderSize
	} else {
		panic("unknown snapshot version")
	}
}

type noopCompactor struct{}

func (noopCompactor) Compact(uint64) error { return nil }

func testSnapshotCanBeSent(t *testing.T, sz uint64, maxWait uint64, mutualTLS bool, fs vfs.FS) {
	handler := newTestMessageHandler()
	trans, nodes, stopper, tt := newTestTransport(handler, mutualTLS, fs)
	defer func() {
		if err := fs.RemoveAll(snapshotDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	defer tt.cleanup()
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	nodes.Add(100, 2, serverAddress)
	t.Logf("going to generate snapshot file")
	tt.generateSnapshotFile(100, 12, testSnapshotIndex, "testsnapshot.gbsnap", sz, fs)
	t.Logf("snapshot file created")
	m := getTestSnapshotMessage(2)
	m.Snapshot.FileSize = getTestSnapshotFileSize(sz)
	dir := tt.GetSnapshotDir(100, 12, testSnapshotIndex)
	snapDir := trans.GetSnapshotDirFunc()(100, 2)
	if err := fs.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("%v", err)
	}
	m.Snapshot.Filepath = fs.PathJoin(dir, "testsnapshot.gbsnap")
	// send the snapshot file
	t.Logf("send snapshot will be called")
	done := trans.SendSnapshot(m)
	t.Logf("send snapshot returned")
	if !done {
		t.Errorf("failed to send the snapshot")
	}
	t.Logf("waiting for snapshot status update")
	waitForFirstSnapshotStatusUpdate(handler, maxWait)
	t.Logf("waiting for snapshot count update")
	waitForSnapshotCountUpdate(handler, maxWait)
	t.Logf("snapshot count updated")
	if handler.getSnapshotCount(100, 2) != 1 {
		t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 1)
	}
	if handler.getFailedSnapshotCount(100, 2) != 0 {
		t.Errorf("got %d, want 0", handler.getFailedSnapshotCount(100, 2))
	}
	if handler.getSnapshotSuccessCount(100, 2) != 1 {
		t.Errorf("got %d, want 1", handler.getSnapshotSuccessCount(100, 2))
	}
	if handler.getReceivedSnapshotFromCount(100, 12) != 1 {
		t.Errorf("got %d, want 1", handler.getReceivedSnapshotFromCount(100, 12))
	}
	if handler.getReceivedSnapshotCount(100, 2) != 1 {
		t.Errorf("got %d, want 1", handler.getReceivedSnapshotFromCount(100, 12))
	}
	md5Original, err := tt.getSnapshotFileMD5(100,
		2, testSnapshotIndex, "testsnapshot.gbsnap")
	if err != nil {
		t.Errorf("err %v, want nil", err)
	}
	md5Received, err := tt.getSnapshotFileMD5(100,
		12, testSnapshotIndex, "testsnapshot.gbsnap")
	if err != nil {
		t.Errorf("err %v, want nil", err)
	}
	if !bytes.Equal(md5Original, md5Received) {
		t.Errorf("snapshot content changed during transmission")
	}
}

func testSnapshotWithNotMatchedDBVWillBeDropped(t *testing.T, f transport.StreamChunkSendFunc, mutualTLS bool, fs vfs.FS) {
	handler := newTestMessageHandler()
	trans, nodes, stopper, tt := newTestTransport(handler, mutualTLS, fs)
	defer tt.cleanup()
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	nodes.Add(100, 2, serverAddress)
	tt.generateSnapshotFile(100, 12, testSnapshotIndex, "testsnapshot.gbsnap", 1024, fs)
	m := getTestSnapshotMessage(2)
	m.Snapshot.FileSize = getTestSnapshotFileSize(1024)
	dir := tt.GetSnapshotDir(100, 12, testSnapshotIndex)
	m.Snapshot.Filepath = fs.PathJoin(dir, "testsnapshot.gbsnap")
	// send the snapshot file
	trans.SetPreStreamChunkSendHook(f)
	done := trans.SendSnapshot(m)
	if !done {
		t.Errorf("failed to send the snapshot")
	}
	waitForFirstSnapshotStatusUpdate(handler, 1000)
	waitForSnapshotCountUpdate(handler, 1000)
	if handler.getSnapshotCount(100, 2) != 0 {
		t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
	}
	// such snapshot dropped on the sending side should be reported.
	if handler.getFailedSnapshotCount(100, 2) != 0 {
		t.Errorf("got %d, want 0", handler.getFailedSnapshotCount(100, 2))
	}
	if handler.getSnapshotSuccessCount(100, 2) != 1 {
		t.Errorf("got %d, want 1", handler.getSnapshotSuccessCount(100, 2))
	}
}

func TestSnapshotWithNotMatchedDeploymentIDWillBeDropped(t *testing.T) {
	fs := vfs.NewStrictMem()
	defer leaktest.AfterTest(t)()
	f := func(c raftpb.Chunk) (raftpb.Chunk, bool) {
		c.DeploymentId = 2
		return c, true
	}
	testSnapshotWithNotMatchedDBVWillBeDropped(t, f, true, fs)
	testSnapshotWithNotMatchedDBVWillBeDropped(t, f, false, fs)
}

func TestSnapshotWithNotMatchedBinVerWillBeDropped(t *testing.T) {
	fs := vfs.NewStrictMem()
	defer leaktest.AfterTest(t)()
	f := func(c raftpb.Chunk) (raftpb.Chunk, bool) {
		c.BinVer = raftio.TransportBinVersion + 1
		return c, true
	}
	testSnapshotWithNotMatchedDBVWillBeDropped(t, f, true, fs)
	testSnapshotWithNotMatchedDBVWillBeDropped(t, f, false, fs)
}

func TestMaxSnapshotConnectionIsLimited(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	trans, nodes, stopper, tt := newTestTransport(handler, false, fs)
	defer tt.cleanup()
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	nodes.Add(100, 2, serverAddress)
	conns := make([]raftpb.IChunkSink, 0)
	for i := uint64(0); i < transport.MaxConnectionCount; i++ {
		sink := trans.GetStreamSink(100, 2)
		if sink == nil {
			t.Errorf("failed to get sink")
		}
		conns = append(conns, sink)
	}
	for i := uint64(0); i < transport.MaxConnectionCount; i++ {
		if sink := trans.GetStreamSink(100, 2); sink != nil {
			t.Errorf("connection is not limited")
		}
	}
	for _, v := range conns {
		close(v.(*transport.Sink).J.Stream)
	}
	for {
		if trans.JobsCount() != 0 {
			time.Sleep(time.Millisecond)
		} else {
			break
		}
	}
	breaker := trans.GetCircuitBreaker(serverAddress)
	breaker.Reset()
	t.Logf("circuit breaker for %s is now ready", serverAddress)
	for i := uint64(0); i < transport.MaxConnectionCount; i++ {
		if sink := trans.GetStreamSink(100, 2); sink == nil {
			t.Fatalf("failed to get sink again %d", i)
		}
	}
}

func testFailedConnectionReportsSnapshotFailure(t *testing.T,
	mutualTLS bool, fs vfs.FS) {
	snapshotSize := transport.SnapshotChunkSize * 10
	handler := newTestMessageHandler()
	trans, nodes, stopper, tt := newTestTransport(handler, mutualTLS, fs)
	defer tt.cleanup()
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	// invalid address
	nodes.Add(100, 2, "localhost:12345")
	tt.generateSnapshotFile(100, 12, testSnapshotIndex, "testsnapshot.gbsnap", snapshotSize, fs)
	m := getTestSnapshotMessage(2)
	m.Snapshot.FileSize = getTestSnapshotFileSize(snapshotSize)
	dir := tt.GetSnapshotDir(100, 12, testSnapshotIndex)
	m.Snapshot.Filepath = fs.PathJoin(dir, "testsnapshot.gbsnap")
	// send the snapshot file
	done := trans.SendSnapshot(m)
	if !done {
		t.Errorf("failed to send the snapshot")
	}
	waitForTotalSnapshotStatusUpdateCount(handler, 6000, 1)
	if handler.getSnapshotCount(100, 2) != 0 {
		t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
	}
	if handler.getFailedSnapshotCount(100, 2) == 0 {
		t.Errorf("got %d, want > 0", handler.getFailedSnapshotCount(100, 2))
	}
}

func TestFailedConnectionReportsSnapshotFailure(t *testing.T) {
	fs := vfs.NewStrictMem()
	defer leaktest.AfterTest(t)()
	testFailedConnectionReportsSnapshotFailure(t, true, fs)
	testFailedConnectionReportsSnapshotFailure(t, false, fs)
}

func testSnapshotWithExternalFilesCanBeSend(t *testing.T,
	sz uint64, maxWait uint64, mutualTLS bool, fs vfs.FS) {
	handler := newTestMessageHandler()
	trans, nodes, stopper, tt := newTestTransport(handler, mutualTLS, fs)
	defer func() {
		if err := fs.RemoveAll(snapshotDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	defer tt.cleanup()
	defer func() {
		if err := trans.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	defer stopper.Stop()
	snapDir := trans.GetSnapshotDirFunc()(100, 2)
	if err := fs.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("%v", err)
	}
	nodes.Add(100, 2, serverAddress)
	tt.generateSnapshotFile(100, 12, testSnapshotIndex, "testsnapshot.gbsnap", sz, fs)
	tt.generateSnapshotExternalFile(100, 12, testSnapshotIndex, "external1.data", sz)
	tt.generateSnapshotExternalFile(100, 12, testSnapshotIndex, "external2.data", sz)
	m := getTestSnapshotMessage(2)
	dir := tt.GetSnapshotDir(100, 12, testSnapshotIndex)
	m.Snapshot.FileSize = getTestSnapshotFileSize(sz)
	m.Snapshot.Filepath = fs.PathJoin(dir, "testsnapshot.gbsnap")
	f1 := &raftpb.SnapshotFile{
		Filepath: fs.PathJoin(dir, "external1.data"),
		FileSize: sz,
		FileId:   1,
	}
	f2 := &raftpb.SnapshotFile{
		Filepath: fs.PathJoin(dir, "external2.data"),
		FileSize: sz,
		FileId:   2,
	}
	m.Snapshot.Files = []*raftpb.SnapshotFile{f1, f2}
	// send the snapshot file
	done := trans.SendSnapshot(m)
	if !done {
		t.Errorf("failed to send the snapshot")
	}
	waitForFirstSnapshotStatusUpdate(handler, maxWait)
	waitForSnapshotCountUpdate(handler, maxWait)
	if handler.getSnapshotCount(100, 2) != 1 {
		t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 1)
	}
	if handler.getFailedSnapshotCount(100, 2) != 0 {
		t.Errorf("got %d, want 0", handler.getFailedSnapshotCount(100, 2))
	}
	if handler.getSnapshotSuccessCount(100, 2) != 1 {
		t.Errorf("got %d, want 1", handler.getSnapshotSuccessCount(100, 2))
	}
	if handler.getReceivedSnapshotFromCount(100, 12) != 1 {
		t.Errorf("got %d, want 1", handler.getReceivedSnapshotFromCount(100, 12))
	}
	if handler.getReceivedSnapshotCount(100, 2) != 1 {
		t.Errorf("got %d, want 1", handler.getReceivedSnapshotFromCount(100, 12))
	}
	filenames := []string{"testsnapshot.gbsnap", "external1.data", "external2.data"}
	for _, fn := range filenames {
		md5Original, err := tt.getSnapshotFileMD5(100, 2, testSnapshotIndex, fn)
		if err != nil {
			t.Errorf("err %v, want nil", err)
		}
		md5Received, err := tt.getSnapshotFileMD5(100, 12, testSnapshotIndex, fn)
		if err != nil {
			t.Errorf("err %v, want nil", err)
		}
		if !bytes.Equal(md5Original, md5Received) {
			t.Errorf("snapshot content changed during transmission")
		}
	}
}

func TestSnapshotWithExternalFilesCanBeSend(t *testing.T) {
	fs := vfs.NewStrictMem()
	testSnapshotWithExternalFilesCanBeSend(t, transport.SnapshotChunkSize/2, 3000, false, fs)
	testSnapshotWithExternalFilesCanBeSend(t, transport.SnapshotChunkSize*3+100, 3000, false, fs)
	testSnapshotWithExternalFilesCanBeSend(t, transport.SnapshotChunkSize/2, 3000, true, fs)
	testSnapshotWithExternalFilesCanBeSend(t, transport.SnapshotChunkSize*3+100, 3000, true, fs)
}

func TestNoOPTransportCanBeCreated(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	tt, _, _, _, _ := newNOOPTestTransport(handler, fs)
	defer func() {
		if err := tt.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
}

func TestInitialMessageCanBeSent(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	tt, nodes, noopTransport, req, connReq := newNOOPTestTransport(handler, fs)
	defer func() {
		if err := tt.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	nodes.Add(100, 2, serverAddress)
	msg := raftpb.Message{
		Type:      raftpb.Heartbeat,
		To:        2,
		ClusterId: 100,
	}
	connReq.SetToFail(false)
	req.SetToFail(false)
	ok := tt.Send(msg)
	if !ok {
		t.Errorf("send failed")
	}
	for i := 0; i < 1000; i++ {
		if atomic.LoadUint64(&noopTransport.Connected) != 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if tt.QueueSize() != 1 {
		t.Errorf("queue len %d, want 1", tt.QueueSize())
	}
	if noopTransport.Connected != 1 {
		t.Errorf("connected %d, want 1", noopTransport.Connected)
	}
}

func TestFailedConnectionIsRemovedFromTransport(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	tt, nodes, _, req, connReq := newNOOPTestTransport(handler, fs)
	defer func() {
		if err := tt.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	nodes.Add(100, 2, serverAddress)
	msg := raftpb.Message{
		Type:      raftpb.Heartbeat,
		To:        2,
		ClusterId: 100,
	}
	connReq.SetToFail(false)
	req.SetToFail(false)
	ok := tt.Send(msg)
	if !ok {
		t.Errorf("send failed")
	}
	req.SetToFail(true)
	// requested the noop trans to fail the send batch operation, given the first
	// batch is in the chan, we don't know whether the first batch is going to
	// fail or the one below is going to fail. after the send below, failure will
	// eventually be triggered and we just need to check & wait.
	tt.Send(msg)
	for i := 0; i < 5000; i++ {
		if tt.QueueSize() != 0 {
			time.Sleep(time.Millisecond)
		} else {
			break
		}
	}
	if tt.QueueSize() != 0 {
		t.Errorf("queue len %d, want 0", tt.QueueSize())
	}
}

func TestCircuitBreakerCauseFailFast(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	tt, nodes, noopTransport, req, connReq := newNOOPTestTransport(handler, fs)
	defer func() {
		if err := tt.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	nodes.Add(100, 2, serverAddress)
	msg := raftpb.Message{
		Type:      raftpb.Heartbeat,
		To:        2,
		ClusterId: 100,
	}
	connReq.SetToFail(false)
	req.SetToFail(false)
	ok := tt.Send(msg)
	if !ok {
		t.Errorf("send failed")
	}
	req.SetToFail(true)
	// see comments in TestFailedConnectionIsRemovedFromTransport for why
	// the returned value of the below Send() is not checked
	tt.Send(msg)
	for i := 0; i < 1000; i++ {
		if tt.QueueSize() != 0 {
			time.Sleep(time.Millisecond)
		} else {
			break
		}
	}
	req.SetToFail(false)
	for i := 0; i < 20; i++ {
		ok = tt.Send(msg)
		if ok {
			t.Errorf("send unexpectedly returned ok")
		}
		time.Sleep(time.Millisecond)
	}
	if tt.QueueSize() != 0 {
		t.Errorf("queue len %d, want 0", tt.QueueSize())
	}
	if atomic.LoadUint64(&noopTransport.Connected) != 1 {
		t.Errorf("connected %d, want 1", noopTransport.Connected)
	}
	if atomic.LoadUint64(&noopTransport.TryConnect) != 1 {
		t.Errorf("connected %d, want 1", noopTransport.TryConnect)
	}
}

func TestCircuitBreakerForResolveNotShared(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	tt, nodes, noopTransport, req, connReq := newNOOPTestTransport(handler, fs)
	defer func() {
		if err := tt.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	nodes.Add(100, 2, serverAddress)
	msg := raftpb.Message{
		Type:      raftpb.Heartbeat,
		To:        2,
		ClusterId: 100,
	}
	msgUnknownNode := raftpb.Message{
		Type:      raftpb.Heartbeat,
		To:        3,
		ClusterId: 100,
	}
	connReq.SetToFail(false)
	req.SetToFail(false)
	if ok := tt.Send(msg); !ok {
		t.Errorf("send failed")
	}
	for i := 0; i < 20; i++ {
		if ok := tt.Send(msgUnknownNode); ok {
			t.Errorf("send unexpectedly returned ok")
		}
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 20; i++ {
		if ok := tt.Send(msg); !ok {
			t.Errorf("send failed for known host")
		}
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadUint64(&noopTransport.Connected) != 1 {
		t.Errorf("connected %d, want 1", noopTransport.Connected)
	}
	if atomic.LoadUint64(&noopTransport.TryConnect) != 1 {
		t.Errorf("connected %d, want 1", noopTransport.TryConnect)
	}
}

// unknown target.
func TestStreamToUnknownTargetWillHaveSnapshotStatusUpdated(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	tt, nodes, _, _, _ := newNOOPTestTransport(handler, fs)
	defer func() {
		if err := tt.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	nodes.Add(100, 2, serverAddress)
	sink := tt.GetStreamSink(100, 3)
	if sink != nil {
		t.Errorf("unexpectedly returned a sink")
	}
	if handler.getFailedSnapshotCount(100, 3) != 1 {
		t.Errorf("snapshot failed count %d", handler.snapshotFailedCount)
	}
	if handler.getSnapshotSuccessCount(100, 3) != 0 {
		t.Errorf("snapshot succeed count %d", handler.snapshotSuccessCount)
	}
}

// failed to connect.
func TestFailedStreamConnectionWillHaveSnapshotStatusUpdated(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	tt, nodes, _, req, connReq := newNOOPTestTransport(handler, fs)
	defer func() {
		if err := tt.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	nodes.Add(100, 2, serverAddress)
	connReq.SetToFail(true)
	req.SetToFail(true)
	tt.GetStreamSink(100, 2)
	failedSnapshotReported := false
	for i := 0; i < 10000; i++ {
		if handler.getFailedSnapshotCount(100, 2) != 1 {
			time.Sleep(time.Millisecond)
			continue
		}
		failedSnapshotReported = true
		break
	}
	if !failedSnapshotReported {
		t.Fatalf("failed snapshot not reported")
	}
}

// failed to connect due to too many connections.
func TestFailedStreamingDueToTooManyConnectionsHaveStatusUpdated(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	tt, nodes, _, _, _ := newNOOPTestTransport(handler, fs)
	defer func() {
		if err := tt.Close(); err != nil {
			t.Fatalf("failed to close the transport module %v", err)
		}
	}()
	nodes.Add(100, 2, serverAddress)
	for i := uint64(0); i < transport.MaxConnectionCount; i++ {
		sink := tt.GetStreamSink(100, 2)
		if sink == nil {
			t.Errorf("failed to connect")
		}
	}
	for i := uint64(0); i < 2*transport.MaxConnectionCount; i++ {
		sink := tt.GetStreamSink(100, 2)
		if sink != nil {
			t.Errorf("stream connection not limited")
		}
	}
	failedSnapshotReported := false
	for i := 0; i < 10000; i++ {
		count := handler.getFailedSnapshotCount(100, 2)
		if count != 2*transport.MaxConnectionCount {
			time.Sleep(time.Millisecond)
			continue
		}
		failedSnapshotReported = true
		break
	}
	if !failedSnapshotReported {
		t.Fatalf("failed snapshot not reported")
	}
}

func TestInMemoryEntrySizeCanBeLimitedWhenSendingMessages(t *testing.T) {
	fs := vfs.NewStrictMem()
	handler := newTestMessageHandler()
	tt, nodes, _, req, _ := newNOOPTestTransport(handler, fs)
	defer func() {
		req.SetBlocked(false)
		defer func() {
			if err := tt.Close(); err != nil {
				t.Fatalf("failed to close the transport module %v", err)
			}
		}()
	}()
	nodes.Add(100, 2, serverAddress)
	e := raftpb.Entry{Cmd: make([]byte, 1024*1024*10)}
	msg := raftpb.Message{
		ClusterId: 100,
		To:        2,
		Type:      raftpb.Replicate,
		Entries:   []raftpb.Entry{e},
	}
	req.SetBlocked(true)
	for i := 0; i < 1000; i++ {
		sent, reason := tt.SendResult(msg)
		if !sent {
			if reason != transport.RateLimited {
				t.Errorf("not due to rate limit")
			}
			break
		}
		if i == 999 {
			t.Errorf("no message rejected")
		}
	}
}
