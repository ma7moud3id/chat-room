# Go RPC Chat Room

Concurrent chat application in Go using a client–server architecture over `net/rpc`.
Communication approach: **Polling** (clients call `Chat.Poll` every 300 ms).

## Run

```bash
go run ./cmd/server                      # terminal 1  (default :1234)
go run ./cmd/client                      # terminal 2, 3, ...  (prompts for username)
go run ./cmd/client -name alice -addr localhost:1234
```

Server flags: `-addr :1234`, `-timeout 6s` (drop a client that stopped polling).
Client flags: `-addr`, `-name`, `-poll 300ms`.

Client commands: type text + Enter to broadcast, `/users` to list who is online, `/quit` (or Ctrl+C / Ctrl+D) to leave.

## Design

| Piece | Where |
|---|---|
| RPC types, shared constants | `chat/types.go` |
| Server state + RPC methods (`Join`, `Leave`, `Send`, `Poll`, `ListUsers`) | `chat/service.go` |
| Server process (listener, one goroutine per connection, Ctrl+C handling) | `cmd/server` |
| Client process (stdin goroutine, polling goroutine, Ctrl+C handling) | `cmd/client` |

- **Concurrency:** all server state (users and their message queues) is guarded by one `sync.Mutex`; each connection is served in its own goroutine.
- **Messaging:** `Send` appends the message to every other user's queue (sender excluded); `Poll` returns and clears the caller's queue.
- **Join/Leave notifications:** queued for all other users on `Join`, `Leave`, or when a client is dropped.
- **Duplicate usernames:** `Join` returns an error; the client asks for another name.
- **Unexpected disconnection:** `Poll` doubles as a heartbeat; a reaper goroutine removes clients silent for longer than `-timeout` and notifies the others.
- **Clean shutdown:** client Ctrl+C sends `Leave` first. Server Ctrl+C queues a shutdown notice for all clients, waits briefly so they receive it, then closes the listener and connections. Clients also exit if the server connection drops.

