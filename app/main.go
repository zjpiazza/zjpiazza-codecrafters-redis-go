package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)

	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		// Read the first line which should be "*1\r\n" for PING
		_, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading command:", err)
			return
		}

		// Read the second line which should be "$4\r\n" for PING
		_, err = reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading command:", err)
			return
		}

		// Read the third line which should be "PING\r\n"
		command, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading command:", err)
			return
		}

		// At this point we've read the entire PING command
		fmt.Println("Received command:", command)

		// Respond with PONG
		conn.Write([]byte("+PONG\r\n"))

		// Continue the loop to handle more commands on this connection
	}
}

func handleCommand(command string, conn net.Conn) {
	switch command {
	default:
		conn.Write([]byte("+PONG\r\n"))
	}
}
