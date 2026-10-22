package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/chat"
)

type client struct {
	rpc      *rpc.Client
	username string
	once     sync.Once
	quit     chan struct{}
}

// shutdown makes sure we only exit once, whatever triggers it.
func (c *client) shutdown(reason string, sendLeave bool) {
	c.once.Do(func() {
		if sendLeave {
			// Best effort: tell the server we're leaving.
			done := make(chan struct{})
			go func() {
				c.rpc.Call("Chat.Leave", &chat.LeaveArgs{Username: c.username}, &chat.LeaveReply{})
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
		}
		c.rpc.Close()
		if reason != "" {
			fmt.Println(reason)
		}
		close(c.quit)
	})
}

func stamp(t time.Time) string { return t.Format("15:04:05") }

func (c *client) poll(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.quit:
			return
		case <-t.C:
			var reply chat.PollReply
			if err := c.rpc.Call("Chat.Poll", &chat.PollArgs{Username: c.username}, &reply); err != nil {
				if err.Error() == chat.ErrNotJoined {
					c.shutdown("** You were removed from the server. **", false)
				} else {
					c.shutdown("** Lost connection to server. **", false)
				}
				return
			}
			for _, m := range reply.Messages {
				switch m.Kind {
				case chat.KindChat:
					fmt.Printf("\r[%s] %s: %s\n", stamp(m.Time), m.From, m.Text)
				case chat.KindNotice:
					fmt.Printf("\r[%s] *** %s ***\n", stamp(m.Time), m.Text)
				case chat.KindShutdown:
					c.shutdown("** Server is shutting down. Goodbye! **", false)
					return
				}
			}
		}
	}
}

func main() {
	addr := flag.String("addr", "localhost:1234", "server address")
	name := flag.String("name", "", "username (prompted if empty)")
	interval := flag.Duration("poll", 300*time.Millisecond, "poll interval")
	flag.Parse()

	in := bufio.NewReader(os.Stdin)
	username := strings.TrimSpace(*name)

	rc, err := rpc.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot connect to server:", err)
		os.Exit(1)
	}

	// Join, re-prompting while the name is rejected.
	var jr chat.JoinReply
	for {
		if username == "" {
			fmt.Print("Enter username: ")
			line, err := in.ReadString('\n')
			if err != nil {
				rc.Close()
				return
			}
			username = strings.TrimSpace(line)
		}
		err := rc.Call("Chat.Join", &chat.JoinArgs{Username: username}, &jr)
		if err == nil {
			break
		}
		if _, isServerErr := err.(rpc.ServerError); !isServerErr {
			fmt.Fprintln(os.Stderr, "connection error:", err)
			os.Exit(1)
		}
		fmt.Println("Cannot join:", err)
		username = ""
		if *name != "" { // non-interactive name given on the command line
			rc.Close()
			os.Exit(1)
		}
	}

	c := &client{rpc: rc, username: username, quit: make(chan struct{})}
	fmt.Printf("Welcome, %s! Online now: %s\n", username, strings.Join(append(jr.Users, username), ", "))
	fmt.Println("Type a message and press Enter. Commands: /users  /quit   (Ctrl+C also leaves cleanly)")

	// Ctrl+C -> leave cleanly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sig:
			c.shutdown("\nLeaving chat...", true)
		case <-c.quit:
		}
	}()

	go c.poll(*interval)

	// Read stdin in its own goroutine so quit can interrupt the main wait.
	go func() {
		sc := bufio.NewScanner(in)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			switch {
			case line == "":
			case line == "/quit":
				c.shutdown("Leaving chat...", true)
				return
			case line == "/users":
				var lr chat.ListReply
				if err := rc.Call("Chat.ListUsers", &chat.ListArgs{Username: username}, &lr); err != nil {
					fmt.Println("error:", err)
					continue
				}
				fmt.Printf("Online (%d): %s\n", len(lr.Users), strings.Join(lr.Users, ", "))
			case strings.HasPrefix(line, "/"):
				fmt.Println("Unknown command. Use /users or /quit")
			default:
				if err := rc.Call("Chat.Send", &chat.SendArgs{Username: username, Text: line}, &chat.SendReply{}); err != nil {
					fmt.Println("error:", err)
				}
			}
		}
		// stdin closed (EOF / Ctrl+D)
		c.shutdown("Leaving chat...", true)
	}()

	<-c.quit
}
