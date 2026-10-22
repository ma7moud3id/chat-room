package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"chatroom/chat"
)

func main() {
	addr := flag.String("addr", ":1234", "address to listen on")
	timeout := flag.Duration("timeout", 6*time.Second, "drop a client that hasn't polled for this long")
	flag.Parse()

	svc := chat.NewService(*timeout)
	srv := rpc.NewServer()
	if err := srv.RegisterName("Chat", svc); err != nil {
		log.Fatal(err)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("chat server listening on %s", ln.Addr())

	var (
		mu    sync.Mutex
		conns = make(map[net.Conn]struct{})
		wg    sync.WaitGroup
	)

	// Accept loop: one goroutine per connection.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed on shutdown
			}
			mu.Lock()
			conns[conn] = struct{}{}
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				srv.ServeConn(conn) // returns when the client disconnects
				mu.Lock()
				delete(conns, conn)
				mu.Unlock()
				conn.Close()
			}()
		}
	}()

	// Clean shutdown on Ctrl+C / SIGTERM.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("shutting down...")

	n := svc.Shutdown()
	if n > 0 {
		// Give clients a moment to poll the shutdown notice.
		time.Sleep(1500 * time.Millisecond)
	}
	ln.Close()
	mu.Lock()
	for c := range conns {
		c.Close()
	}
	mu.Unlock()
	wg.Wait()
	log.Println("server stopped")
}
