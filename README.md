Go RPC Chat Room

A concurrent terminal-based chat application built with Go using net/rpc and a client-server architecture.

Features

* Multiple clients connected to one server.
* Unique usernames and duplicate username rejection.
* Broadcast messages to all other connected users.
* Join and leave notifications.
* List online users.
* Concurrent communication using goroutines and synchronization.
* Polling for automatic message reception.
* Graceful client disconnection.

Requirements

* Go installed on your computer.

How to Run

Open a terminal and navigate to the chatroom directory:

cd chatroom

Start the server in Terminal 1:

go run ./cmd/server

Start each client in a separate terminal:

go run ./cmd/client

Run multiple clients and choose a different username for each one.

Client Commands

* /users — List connected users.
* /quit — Leave the chat.
* Type any message and press Enter to send it to the other connected clients.

Communication

The application uses Go’s net/rpc package and polling to retrieve messages and notifications from the server.

Repository Structure

The source code is located in the chatroom directory.
