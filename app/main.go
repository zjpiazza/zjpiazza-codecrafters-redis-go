package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var db map[string]interface{}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Load db if exists
	db = loadDatabase()

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
		command, args, err := parseCommand(reader, conn)
		if err != nil {
			fmt.Println("Error parsing command:", err)
			return
		}
		executeCommand(command, args, conn)
	}
}

func loadDatabase() map[string]interface{} {
	if _, err := os.Stat("db.json"); errors.Is(err, os.ErrNotExist) {
		return make(map[string]interface{})
	} else {
		db, err := readMapFromJSON("db.json")
		if err != nil {
			fmt.Println("Error loading database:", err)
			return make(map[string]interface{})
		}
		return db
	}
}

func sendError(err error, conn net.Conn) {
	errMsg := fmt.Sprintf("-ERR %s\r\n", err.Error())
	conn.Write([]byte(errMsg))
}

// Parse RESP protocol message
func parseCommand(reader *bufio.Reader, conn net.Conn) (string, []string, error) {
	arrayHeader, err := reader.ReadString('\n')
	if err != nil {
		sendError(err, conn)
		return "", nil, err
	}

	count, valid := parseArrayHeader(arrayHeader)
	if !valid {
		err := errors.New("invalid array header")
		sendError(err, conn)
		return "", nil, err
	}

	// Read all elements (command + args)
	var command string
	var arguments []string

	for i := 0; i < count; i++ {
		element, err := parseBulkString(reader, conn)
		if err != nil {
			return "", nil, err
		}

		if i == 0 {
			command = strings.ToUpper(element) // Commands are case-insensitive
		} else {
			arguments = append(arguments, element)
		}
	}

	return command, arguments, nil
}

func formatBulkString(text string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(text), text)
}

func parseBulkString(reader *bufio.Reader, conn net.Conn) (string, error) {
	bulkStringHeader, err := reader.ReadString('\n')
	if err != nil {
		sendError(err, conn)
		return "", err
	}

	bulkStringHeaderPattern := regexp.MustCompile(`^\$(\d+)\r\n$`)
	matches := bulkStringHeaderPattern.FindStringSubmatch(bulkStringHeader)

	if len(matches) < 2 {
		err := errors.New("invalid bulk string header")
		sendError(err, conn)
		return "", err
	}

	count, err := strconv.Atoi(matches[1])
	if err != nil {
		sendError(errors.New("invalid bulk string header"), conn)
		return "", err
	}

	// Read exactly count bytes plus \r\n
	bulkString := make([]byte, count+2) // +2 for \r\n
	_, err = io.ReadFull(reader, bulkString)
	if err != nil {
		sendError(err, conn)
		return "", err
	}

	// Remove the trailing \r\n
	payload := string(bulkString[:count])

	// Verify the last two bytes are \r\n
	if string(bulkString[count:]) != "\r\n" {
		err := errors.New("bulk string missing terminator")
		sendError(err, conn)
		return "", err
	}

	return payload, nil
}

func parseArrayHeader(line string) (int, bool) {
	// Regex with a capturing group for the digits
	pattern := regexp.MustCompile(`^\*(\d+)\r\n$`)

	// Find matches
	matches := pattern.FindStringSubmatch(line)

	// If there are no matches or not enough capturing groups, return false
	if len(matches) < 2 {
		return 0, false
	}

	// Convert the captured digits to an integer
	count, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, false
	}

	return count, true
}

func executeCommand(command string, args []string, conn net.Conn) {
	switch command {
	case "PING":
		// Respond with RESP simple string
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		if len(args) < 1 {
			sendError(errors.New("wrong number of arguments for 'echo' command"), conn)
			return
		}
		data := args[0]
		conn.Write([]byte(formatBulkString(data)))
	case "SET":
		if len(args) < 2 {
			sendError(errors.New("wrong number of arguments for 'set' command"), conn)
			return
		}
		key := args[0]
		value := args[1]
		db[key] = value

		// Check for optional arguments (EX, PX, etc.)
		// Not implemented in this basic version

		err := writeMapToJSON(db, "db.json")
		if err != nil {
			fmt.Println("Error writing to database:", err)
		}
		conn.Write([]byte("+OK\r\n"))
	case "GET":
		if len(args) < 1 {
			sendError(errors.New("wrong number of arguments for 'get' command"), conn)
			return
		}
		key := args[0]
		value, exists := db[key]
		if !exists || value == nil {
			// Return Redis null bulk string for non-existent keys
			conn.Write([]byte("$-1\r\n"))
			return
		}

		// Safe type assertion
		strValue, ok := value.(string)
		if !ok {
			conn.Write([]byte("$-1\r\n"))
			return
		}

		conn.Write([]byte(formatBulkString(strValue)))
	default:
		sendError(errors.New("unknown command '"+command+"'"), conn)
	}
}

func writeMapToJSON(data map[string]interface{}, filename string) error {
	// Marshal the map to JSON
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	// Write JSON to file
	return os.WriteFile(filename, jsonData, 0644)
}

func readMapFromJSON(filename string) (map[string]interface{}, error) {
	// Read file
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	// Unmarshal JSON to map
	var result map[string]interface{}
	err = json.Unmarshal(jsonData, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
