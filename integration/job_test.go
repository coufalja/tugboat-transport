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
	"context"
	"testing"

	"github.com/coufalja/tugboat-transport/noop"
	pb "github.com/coufalja/tugboat/raftpb"
	"github.com/coufalja/tugboat/transport"
	"github.com/lni/goutils/syncutil"
	"github.com/lni/vfs"
)

func TestSnapshotJobCanBeCreatedInSavedMode(t *testing.T) {
	fs := vfs.NewStrictMem()
	tr := noop.New()
	c := transport.NewJob(context.Background(), 1, 1, 1, false, 201, tr, nil, fs)
	if cap(c.Stream) != 201 {
		t.Errorf("unexpected chan length %d, want 201", cap(c.Stream))
	}
}

func TestSnapshotJobCanBeCreatedInStreamingMode(t *testing.T) {
	fs := vfs.NewStrictMem()
	tr := noop.New()
	c := transport.NewJob(context.Background(), 1, 1, 1, true, 201, tr, nil, fs)
	if cap(c.Stream) != transport.StreamingChanLength {
		t.Errorf("unexpected chan length %d, want %d", cap(c.Stream), transport.StreamingChanLength)
	}
}

func TestSendSavedSnapshotPutsAllChunksInCh(t *testing.T) {
	fs := vfs.NewStrictMem()
	m := pb.Message{
		Type: pb.InstallSnapshot,
		Snapshot: pb.Snapshot{
			FileSize: 1024 * 1024 * 512,
		},
	}
	chunks, err := transport.SplitSnapshotMessage(m)
	if err != nil {
		t.Fatalf("failed to get chunks %v", err)
	}
	tr := noop.New()
	c := transport.NewJob(context.Background(), 1, 1, 1, false, len(chunks), tr, nil, fs)
	if cap(c.Stream) != len(chunks) {
		t.Errorf("unexpected chan length %d", cap(c.Stream))
	}
	c.AddSnapshot(chunks)
	if len(c.Stream) != len(chunks) {
		t.Errorf("not all chunks pushed to Stream")
	}
}

func TestKeepSendingChunksUsingFailedJobWillNotBlock(t *testing.T) {
	fs := vfs.NewStrictMem()
	tr := noop.New()
	c := transport.NewJob(context.Background(), 1, 1, 1, true, 0, tr, nil, fs)
	if cap(c.Stream) != transport.StreamingChanLength {
		t.Errorf("unexpected chan length %d, want %d", cap(c.Stream), transport.StreamingChanLength)
	}
	if err := c.Connect("a1"); err != nil {
		t.Fatalf("connect failed %v", err)
	}
	stopper := syncutil.NewStopper()
	var perr error
	stopper.RunWorker(func() {
		perr = c.Process()
	})
	noopConn, ok := c.Conn.(*noop.SnapshotConnection)
	if !ok {
		t.Fatalf("failed to get noopConn")
	}
	noopConn.Req.SetToFail(true)
	sent, stopped := c.AddChunk(pb.Chunk{})
	if !sent {
		t.Fatalf("failed to SendResult")
	}
	if stopped {
		t.Errorf("unexpectedly stopped")
	}
	stopper.Stop()
	if perr == nil {
		t.Fatalf("error didn't return from process()")
	}
	for i := 0; i < transport.StreamingChanLength*10; i++ {
		c.AddChunk(pb.Chunk{})
	}
	c.Close()
}

func testSpecialChunkCanStopTheProcessLoop(t *testing.T,
	tt uint64, experr error, fs vfs.FS) {
	tr := noop.New()
	c := transport.NewJob(context.Background(), 1, 1, 1, true, 0, tr, nil, fs)
	if err := c.Connect("a1"); err != nil {
		t.Fatalf("connect failed %v", err)
	}
	stopper := syncutil.NewStopper()
	var perr error
	stopper.RunWorker(func() {
		perr = c.Process()
	})
	poison := pb.Chunk{
		ChunkCount: tt,
	}
	sent, stopped := c.AddChunk(poison)
	if !sent {
		t.Fatalf("failed to SendResult")
	}
	if stopped {
		t.Errorf("unexpectedly stopped")
	}
	stopper.Stop()
	if perr != experr {
		t.Errorf("unexpected error val %v", perr)
	}
}

func TestPoisonChunkCanStopTheProcessLoop(t *testing.T) {
	fs := vfs.NewStrictMem()
	testSpecialChunkCanStopTheProcessLoop(t,
		pb.PoisonChunkCount, transport.ErrStreamSnapshot, fs)
}

func TestLastChunkCanStopTheProcessLoop(t *testing.T) {
	fs := vfs.NewStrictMem()
	testSpecialChunkCanStopTheProcessLoop(t, pb.LastChunkCount, nil, fs)
}
