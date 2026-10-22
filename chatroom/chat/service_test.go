package chat

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDuplicateAndInvalidNames(t *testing.T) {
	s := NewService(time.Second)
	defer s.Shutdown()
	if err := s.Join(&JoinArgs{"alice"}, &JoinReply{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Join(&JoinArgs{"alice"}, &JoinReply{}); err == nil || err.Error() != ErrUsernameTaken {
		t.Fatalf("want duplicate error, got %v", err)
	}
	if err := s.Join(&JoinArgs{"bad name!"}, &JoinReply{}); err == nil {
		t.Fatal("want invalid name error")
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	s := NewService(time.Second)
	defer s.Shutdown()
	s.Join(&JoinArgs{"a"}, &JoinReply{})
	s.Join(&JoinArgs{"b"}, &JoinReply{})
	s.Send(&SendArgs{"a", "hi"}, &SendReply{})

	var ra, rb PollReply
	s.Poll(&PollArgs{"a"}, &ra)
	s.Poll(&PollArgs{"b"}, &rb)
	for _, m := range ra.Messages {
		if m.Kind == KindChat {
			t.Fatal("sender received own message")
		}
	}
	got := false
	for _, m := range rb.Messages {
		if m.Kind == KindChat && m.From == "a" && m.Text == "hi" {
			got = true
		}
	}
	if !got {
		t.Fatal("b did not receive message")
	}
}

func TestConcurrentUse(t *testing.T) {
	s := NewService(time.Second)
	defer s.Shutdown()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("u%d", i)
			if err := s.Join(&JoinArgs{name}, &JoinReply{}); err != nil {
				t.Error(err)
				return
			}
			for j := 0; j < 50; j++ {
				s.Send(&SendArgs{name, "x"}, &SendReply{})
				s.Poll(&PollArgs{name}, &PollReply{})
				s.ListUsers(&ListArgs{name}, &ListReply{})
			}
			s.Leave(&LeaveArgs{name}, &LeaveReply{})
		}(i)
	}
	wg.Wait()
	s.mu.Lock()
	n := len(s.users)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d users left behind", n)
	}
}

func TestReaperRemovesSilentClient(t *testing.T) {
	s := NewService(200 * time.Millisecond)
	defer s.Shutdown()
	s.Join(&JoinArgs{"ghost"}, &JoinReply{})
	s.Join(&JoinArgs{"watcher"}, &JoinReply{})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var r PollReply
		s.Poll(&PollArgs{"watcher"}, &r) // watcher keeps heartbeating
		for _, m := range r.Messages {
			if m.Kind == KindNotice && m.Text == "ghost left (connection lost)" {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("ghost was never reaped")
}
