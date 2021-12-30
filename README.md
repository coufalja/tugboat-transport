# tugboat-transport

This is the default transport implementation for the [Tugboat](https://github.com/coufalja/tugboat) library.

## How to use

```go
package main

import (
	"github.com/coufalja/tugboat"
	"github.com/coufalja/tugboat/config"
	"github.com/coufalja/tugboat-transport/tcp" // Import the tcp package
)

func main() {
	nhc := config.NodeHostConfig{
		NodeHostDir:    "/tmp",
		RTTMillisecond: 50,
		RaftAddress:    "localhost:8079",
	}

	// Configure the transport
	cfg := tcp.Config{
		MaxSnapshotSendBytesPerSecond: 5 * 1024 * 1024,
		MaxSnapshotRecvBytesPerSecond: 5 * 1024 * 1024,
		MutualTLS:                     false,
		RaftAddress:                   "localhost:8079", // The same RaftAddress must be passed both to transport and the NodeHost
	}
	_, _ = tugboat.NewNodeHost(nhc, tcp.Factory(cfg))
}
```
