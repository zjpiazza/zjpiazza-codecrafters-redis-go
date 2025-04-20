package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
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

	for {
		go handleCommand(conn)
	}
}

func handleCommand(conn net.Conn) {
	reader := bufio.NewReader(conn)

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
	command = strings.TrimSuffix(command, "\r\n")
	if err != nil {
		fmt.Println("Error reading command:", err)
		return
	}

	switch command {
	case "PING":
		// Respond with RESP simple string
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		// Respond with RESP bulk string
		// Read the next line to grab the data
		length, _ := reader.ReadString('\n')
		length = strings.TrimSuffix(length, "\r\n")
		length = strings.TrimPrefix(length, "$")
		lengthParsed, _ := strconv.Atoi(length)

		data, _ := reader.ReadString('\n')
		data = strings.TrimSuffix(data, "\r\n")

		fmt.Println(length)
		fmt.Println(data)

		response := fmt.Sprintf("$%d\r\n%s\r\n", lengthParsed, data)
		fmt.Println(response)

		conn.Write([]byte(response))
	default:
		fmt.Printf("Fall through statement on command: %s", command)
		fmt.Print("ECHO" == command)
		fmt.Println(reflect.TypeOf("ECHO"))
		fmt.Println(reflect.TypeOf(command))
	}
}
