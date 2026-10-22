package chat

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"
)

var nameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,20}$`)

// maxQueue bounds the per-client backlog so a stalled client can't grow
// server memory without limit.
const maxQueue = 1000

type user struct {
	queue    []Message
	lastSeen time.Time
}

// Service is the object registered with net/rpc. All state is protected by mu.
type Service struct {
	mu      sync.Mutex
	users   map[string]*user
	timeout time.Duration // client considered dead if it hasn't polled for this long
	stop    chan struct{}
	done    chan struct{}
}

// NewService creates the service and starts the reaper goroutine that removes
// clients that vanished without calling Leave (crash, network loss, kill -9).
func NewService(clientTimeout time.Duration) *Service {
	s := &Service{
		users:   make(map[string]*user),
		timeout: clientTimeout,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go s.reaper()
	return s
}

func (s *Service) reaper() {
	defer close(s.done)
	t := time.NewTicker(s.timeout / 4)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-t.C:
			s.mu.Lock()
			for name, u := range s.users {
				if now.Sub(u.lastSeen) > s.timeout {
					delete(s.users, name)
					s.broadcastLocked("", notice(fmt.Sprintf("%s left (connection lost)", name)))
				}
			}
			s.mu.Unlock()
		}
	}
}

func notice(text string) Message {
	return Message{Time: time.Now(), Kind: KindNotice, Text: text}
}

// broadcastLocked appends m to every user's queue except `except`.
// Caller must hold s.mu.
func (s *Service) broadcastLocked(except string, m Message) {
	for name, u := range s.users {
		if name == except {
			continue
		}
		if len(u.queue) >= maxQueue {
			u.queue = u.queue[1:] // drop oldest
		}
		u.queue = append(u.queue, m)
	}
}

func (s *Service) namesLocked() []string {
	names := make([]string, 0, len(s.users))
	for n := range s.users {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ---- RPC methods ----

func (s *Service) Join(args *JoinArgs, reply *JoinReply) error {
	if !nameRe.MatchString(args.Username) {
		return errors.New(ErrInvalidName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.users[args.Username]; exists {
		return errors.New(ErrUsernameTaken)
	}
	reply.Users = s.namesLocked() // before adding the new user
	s.users[args.Username] = &user{lastSeen: time.Now()}
	s.broadcastLocked(args.Username, notice(args.Username+" joined the chat"))
	return nil
}

func (s *Service) Leave(args *LeaveArgs, _ *LeaveReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[args.Username]; !ok {
		return errors.New(ErrNotJoined)
	}
	delete(s.users, args.Username)
	s.broadcastLocked("", notice(args.Username+" left the chat"))
	return nil
}

func (s *Service) Send(args *SendArgs, _ *SendReply) error {
	if args.Text == "" {
		return errors.New(ErrEmptyMessage)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[args.Username]
	if !ok {
		return errors.New(ErrNotJoined)
	}
	u.lastSeen = time.Now()
	// Sender is excluded from the broadcast.
	s.broadcastLocked(args.Username, Message{
		Time: time.Now(), Kind: KindChat, From: args.Username, Text: args.Text,
	})
	return nil
}

// Poll returns (and clears) the user's pending messages. Calling it also acts
// as a heartbeat.
func (s *Service) Poll(args *PollArgs, reply *PollReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[args.Username]
	if !ok {
		return errors.New(ErrNotJoined)
	}
	u.lastSeen = time.Now()
	reply.Messages = u.queue
	u.queue = nil
	return nil
}

func (s *Service) ListUsers(args *ListArgs, reply *ListReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[args.Username]
	if !ok {
		return errors.New(ErrNotJoined)
	}
	u.lastSeen = time.Now()
	reply.Users = s.namesLocked()
	return nil
}

// Shutdown queues a shutdown message for every client (delivered on their next
// poll) and stops the reaper. It returns how many clients were notified.
func (s *Service) Shutdown() int {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	s.broadcastLocked("", Message{Time: time.Now(), Kind: KindShutdown, Text: "server is shutting down"})
	return len(s.users)
}
