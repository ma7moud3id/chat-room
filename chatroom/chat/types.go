// Package chat contains the RPC types and the server-side service shared by
// the chat server and chat clients.
package chat

import "time"

// MessageKind tells the client how to display a message.
type MessageKind int

const (
	KindChat     MessageKind = iota // normal message from a user
	KindNotice                      // join / leave notification
	KindShutdown                    // server is going down
)

// Message is one item delivered to a client through Poll.
type Message struct {
	Time time.Time
	Kind MessageKind
	From string // empty for notices
	Text string
}

// ---- RPC argument / reply types ----

type JoinArgs struct{ Username string }
type JoinReply struct{ Users []string } // users already online

type LeaveArgs struct{ Username string }
type LeaveReply struct{}

type SendArgs struct {
	Username string
	Text     string
}
type SendReply struct{}

type PollArgs struct{ Username string }
type PollReply struct{ Messages []Message }

type ListArgs struct{ Username string }
type ListReply struct{ Users []string }

// Error strings returned by the server (compared by text on the client,
// because net/rpc flattens errors into strings).
const (
	ErrUsernameTaken = "username already taken"
	ErrNotJoined     = "not joined (disconnected from server)"
	ErrInvalidName   = "invalid username (use 1-20 letters, digits, '_' or '-')"
	ErrEmptyMessage  = "empty message"
)
