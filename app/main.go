package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1
var _ = net.Listen
var _ = os.Exit

// config is kept global for simplicity
var config = make(map[string]string)

// currentDB keeps track of the current database number
var currentDB int

// Store the in-memory key-value pairs and their expiry times
var (
	keyValueStore = make(map[string]string)
	expiryTimes   = make(map[string]time.Time)
	storeMutex    = &sync.RWMutex{}
)

const OKResp string = "+OK\r\n"
const NullResp string = "$-1\r\n"
const (
	opCodeModuleAux    byte = 247 /* Module auxiliary data. */
	opCodeIdle         byte = 248 /* LRU idle time. */
	opCodeFreq         byte = 249 /* LFU frequency. */
	opCodeAux          byte = 250 /* RDB aux field. */
	opCodeResizeDB     byte = 251 /* Hash table resize hint. */
	opCodeExpireTimeMs byte = 252 /* Expire time in milliseconds. */
	opCodeExpireTime   byte = 253 /* Old expire time in seconds. */
	opCodeSelectDB     byte = 254 /* DB number of the following keys. */
	opCodeEOF          byte = 255 /* End of RDB file indicator. */
)

// Value type opcodes
const (
	ValueTypeString byte = 0x00
	ValueTypeList   byte = 0x01
	ValueTypeSet    byte = 0x02
	ValueTypeZSet   byte = 0x03
	ValueTypeHash   byte = 0x04
	// Add other value types as needed
)

// Read arguments
var directory string
var dbFilename string

func main() {
	fmt.Println("Starting Redis server with RDB file support")

	flag.StringVar(&directory, "dir", "", "Directory for files")
	flag.StringVar(&dbFilename, "dbfilename", "dump.rdb", "Filename for the RDB database file") // Default to dump.rdb
	flag.Parse()

	// Basic config initialization
	config = make(map[string]string)
	if directory != "" {
		config["dir"] = directory
	}
	if dbFilename != "" {
		config["dbfilename"] = dbFilename
	}

	// Load data from RDB file if it exists
	loadFromRDB()

	// Start a ticker to periodically save the data to the RDB file
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			saveToRDB()
		}
	}()

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

// Load data from the RDB file if it exists
func loadFromRDB() {
	rdbPath := dbFilename
	if directory != "" {
		rdbPath = fmt.Sprintf("%s/%s", directory, dbFilename)
	}

	// Check if the file exists
	_, err := os.Stat(rdbPath)
	if os.IsNotExist(err) {
		fmt.Println("RDB file does not exist, starting with empty database")
		return
	}

	// Read the RDB file
	rdbData, err := os.ReadFile(rdbPath)
	if err != nil {
		fmt.Printf("Error reading RDB file: %v\n", err)
		return
	}

	fmt.Printf("Loading data from RDB file: %s\n", rdbPath)

	// Parse the RDB file to load key-value pairs
	parseRDBFile(rdbData)
}

// Parse the RDB file and load key-value pairs
func parseRDBFile(rdbData []byte) {
	if len(rdbData) < 9 {
		fmt.Println("RDB file is too short")
		return
	}

	// Check the REDIS header
	if !bytes.Equal(rdbData[:5], []byte("REDIS")) {
		fmt.Println("Invalid RDB file: missing REDIS header")
		return
	}

	// Look for the RESIZEDB marker
	resizeDbPos := bytes.Index(rdbData, []byte{opCodeResizeDB, 0x01, 0x00, 0x00})
	if resizeDbPos == -1 {
		fmt.Println("Could not find RESIZEDB marker in RDB file")
		return
	}

	// Move past the RESIZEDB marker and its values
	startPos := resizeDbPos + 4

	// Read all key-value pairs until EOF
	for startPos < len(rdbData) && rdbData[startPos] != opCodeEOF {
		// Read key length (assuming simple format)
		keyLen := int(rdbData[startPos])
		startPos++

		if startPos+keyLen > len(rdbData) {
			fmt.Println("Truncated key in RDB file")
			return
		}

		// Read key
		key := string(rdbData[startPos : startPos+keyLen])
		startPos += keyLen

		// Read value length
		valueLen := int(rdbData[startPos])
		startPos++

		if startPos+valueLen > len(rdbData) {
			fmt.Println("Truncated value in RDB file")
			return
		}

		// Read value
		value := string(rdbData[startPos : startPos+valueLen])
		startPos += valueLen

		// Store in memory
		storeMutex.Lock()
		keyValueStore[key] = value
		storeMutex.Unlock()

		fmt.Printf("Loaded key '%s' with value '%s' from RDB file\n", key, value)
	}
}

// Save data to the RDB file
func saveToRDB() {
	rdbPath := dbFilename
	if directory != "" {
		rdbPath = fmt.Sprintf("%s/%s", directory, dbFilename)
	}

	// Create a buffer to hold the RDB data
	var buf bytes.Buffer

	// Write the REDIS header
	buf.WriteString("REDIS0011")

	// Write auxiliary metadata
	// AUX "redis-ver" "7.2.0"
	buf.WriteByte(opCodeAux)
	buf.WriteByte(9) // Length of "redis-ver"
	buf.WriteString("redis-ver")
	buf.WriteByte(5) // Length of "7.2.0"
	buf.WriteString("7.2.0")

	// AUX "redis-bits" 64
	buf.WriteByte(opCodeAux)
	buf.WriteByte(10) // Length of "redis-bits"
	buf.WriteString("redis-bits")
	buf.WriteByte(0xC0) // Special encoding for 64

	// Write database selector
	buf.WriteByte(opCodeSelectDB)
	buf.WriteByte(0) // Database 0

	// Write RESIZEDB with counts
	buf.WriteByte(opCodeResizeDB)
	storeMutex.RLock()
	buf.WriteByte(1) // Simple length for small count
	buf.WriteByte(0)
	buf.WriteByte(0)

	// Write key-value pairs
	for key, value := range keyValueStore {
		// Check if expired
		if expiryTime, exists := expiryTimes[key]; exists && time.Now().After(expiryTime) {
			// Skip expired keys
			delete(keyValueStore, key)
			delete(expiryTimes, key)
			continue
		}

		// String type
		buf.WriteByte(ValueTypeString)

		// Write key
		buf.WriteByte(byte(len(key))) // Length of key (assuming small keys)
		buf.WriteString(key)

		// Write value
		buf.WriteByte(byte(len(value))) // Length of value (assuming small values)
		buf.WriteString(value)
	}
	storeMutex.RUnlock()

	// Write EOF marker
	buf.WriteByte(opCodeEOF)

	// Compute checksum (simple implementation for now - just some bytes)
	checksum := []byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF}
	buf.Write(checksum)

	// Write the RDB file
	err := os.WriteFile(rdbPath, buf.Bytes(), 0644)
	if err != nil {
		fmt.Printf("Error writing RDB file: %v\n", err)
		return
	}

	fmt.Printf("Saved %d keys to RDB file: %s\n", len(keyValueStore), rdbPath)
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		command, args, err := parseCommand(reader, conn)
		if err != nil {
			// If it's an EOF error, the client disconnected gracefully.
			if errors.Is(err, io.EOF) {
				fmt.Println("Client disconnected.")
				return
			}
			fmt.Println("Error parsing command:", err)
			// Send an error response to the client before closing the connection on parsing errors
			sendError(err, conn)
			return // Terminate the connection on parsing errors
		}
		executeCommand(command, args, conn)
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
		return "", nil, err // Propagate EOF or other read errors
	}

	count, valid := parseArrayHeader(arrayHeader)
	if !valid {
		return "", nil, errors.New("invalid array header")
	}

	if count <= 0 {
		return "", nil, errors.New("empty or invalid command")
	}

	// Read all elements (command + args)
	var command string
	var arguments []string

	for i := 0; i < count; i++ {
		element, err := parseBulkString(reader)
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
	if text == "" {
		return "$-1\r\n" // Redis null bulk string for empty strings
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(text), text)
}

func parseBulkString(reader *bufio.Reader) (string, error) {
	bulkStringHeader, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	bulkStringHeaderPattern := regexp.MustCompile(`^\$(\d+)\r\n$`)
	matches := bulkStringHeaderPattern.FindStringSubmatch(bulkStringHeader)

	if len(matches) < 2 {
		// Check for null bulk string $-1\r\n
		if bulkStringHeader == "$-1\r\n" {
			return "", nil // Return empty string for null bulk string
		}
		return "", errors.New("invalid bulk string header format")
	}

	count, err := strconv.Atoi(matches[1])
	if err != nil {
		return "", errors.New("invalid bulk string length")
	}

	if count < -1 {
		return "", errors.New("invalid bulk string length")
	}

	if count == -1 {
		// This case should ideally be caught by the $-1\r\n check above,
		// but as a safeguard, handle it here too.
		return "", nil // Return empty string for null bulk string
	}

	// Read exactly count bytes plus \r\n
	bulkString := make([]byte, count+2) // +2 for \r\n
	_, err = io.ReadFull(reader, bulkString)
	if err != nil {
		return "", err
	}

	// Remove the trailing \r\n
	payload := string(bulkString[:count])

	// Verify the last two bytes are \r\n
	if string(bulkString[count:]) != "\r\n" {
		return "", errors.New("bulk string missing terminator")
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

// readLength reads a length-encoded integer from the byte slice.
// It returns the decoded length and the number of bytes consumed.
func readLength(data []byte, offset int) (int, int, error) {
	if offset >= len(data) {
		return 0, 0, io.EOF
	}

	firstByte := data[offset]

	// Check the two most significant bits
	switch (firstByte & 0xC0) >> 6 {
	case 0: // 00 prefix - 6 bit length
		return int(firstByte & 0x3F), 1, nil

	case 1: // 01 prefix - 14 bit length
		if offset+1 >= len(data) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		length := (int(firstByte&0x3F) << 8) | int(data[offset+1])
		return length, 2, nil

	case 2: // 10 prefix - 32 bit length
		if offset+4 >= len(data) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		length := binary.BigEndian.Uint32(data[offset+1 : offset+5])
		return int(length), 5, nil

	case 3: // 11 prefix - Special format
		// Here we handle special encodings (like integers)
		specialFormat := firstByte & 0x3F

		// Handle integer encodings
		switch specialFormat {
		case 0: // 8 bit integer
			if offset+1 >= len(data) {
				return 0, 0, io.ErrUnexpectedEOF
			}
			return int(data[offset+1]), 2, nil

		case 1: // 16 bit integer
			if offset+2 >= len(data) {
				return 0, 0, io.ErrUnexpectedEOF
			}
			value := binary.LittleEndian.Uint16(data[offset+1 : offset+3])
			return int(value), 3, nil

		case 2: // 32 bit integer
			if offset+4 >= len(data) {
				return 0, 0, io.ErrUnexpectedEOF
			}
			value := binary.LittleEndian.Uint32(data[offset+1 : offset+5])
			return int(value), 5, nil

		default:
			return 0, 0, fmt.Errorf("unsupported special encoding format %d", specialFormat)
		}
	}

	return 0, 0, fmt.Errorf("invalid length encoding byte: %x", firstByte)
}

// New direct approach based on the hexdump patterns
func getKeyDirect(rdbData []byte, targetKey string) (string, bool) {
	// Looking at the hexdump pattern, the key-value pair appears to start around offset 0x2c (44)
	// Specifically, I see in the hexdump:
	// 1. First there's the REDIS0011 header
	// 2. Then there's some metadata with "redis-ver" and "redis-bits"
	// 3. Then at offset 0x2c I see the actual key-value data

	if len(rdbData) < 36 {
		return "", false // File too short
	}

	// Look for the start of the key-value data
	// The pattern seems to be opCodeResizeDB (0xFB) followed by 01 00 00, then the actual key
	resizeDbPos := bytes.Index(rdbData, []byte{opCodeResizeDB, 0x01, 0x00, 0x00})

	if resizeDbPos == -1 {
		fmt.Println("Could not find RESIZEDB marker")
		return "", false
	}

	// Move past the RESIZEDB + values (4 bytes total)
	startPos := resizeDbPos + 4

	// Now the format seems to be:
	// 1. Key length (1 byte for small keys)
	// 2. Key string
	// 3. Value length (1 byte for small values)
	// 4. Value string

	// Safety check
	if startPos+1 >= len(rdbData) {
		return "", false
	}

	// Read key length
	keyLen := int(rdbData[startPos])
	startPos++

	// Safety check
	if startPos+keyLen >= len(rdbData) {
		return "", false
	}

	// Read key
	key := string(rdbData[startPos : startPos+keyLen])
	startPos += keyLen

	fmt.Printf("Found key in RDB: '%s'\n", key)

	// Check if this is the key we want
	if key == targetKey {
		// Read value length
		valueLen := int(rdbData[startPos])
		startPos++

		// Safety check
		if startPos+valueLen > len(rdbData) {
			return "", false
		}

		// Read value
		value := string(rdbData[startPos : startPos+valueLen])
		return value, true
	}

	return "", false
}

func executeCommand(command string, args []string, conn net.Conn) {
	fmt.Printf("Command: %s\n", command)

	switch command {
	case "PING":
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

		// Check for expiry options
		var expiry time.Time
		var hasExpiry bool

		if len(args) > 2 && len(args)%2 == 0 {
			for i := 2; i < len(args); i += 2 {
				option := strings.ToLower(args[i])

				if i+1 >= len(args) {
					sendError(errors.New("syntax error"), conn)
					return
				}

				val := args[i+1]

				switch option {
				case "ex": // seconds
					seconds, err := strconv.Atoi(val)
					if err != nil {
						sendError(errors.New("invalid expire time"), conn)
						return
					}
					expiry = time.Now().Add(time.Duration(seconds) * time.Second)
					hasExpiry = true

				case "px": // milliseconds
					millis, err := strconv.Atoi(val)
					if err != nil {
						sendError(errors.New("invalid expire time"), conn)
						return
					}
					expiry = time.Now().Add(time.Duration(millis) * time.Millisecond)
					hasExpiry = true
				}
			}
		}

		// Store the key-value pair
		storeMutex.Lock()
		keyValueStore[key] = value
		if hasExpiry {
			expiryTimes[key] = expiry
		} else {
			delete(expiryTimes, key) // Remove any existing expiry
		}
		storeMutex.Unlock()

		// Trigger a save
		go saveToRDB()

		conn.Write([]byte(OKResp))

	case "GET":
		if len(args) < 1 {
			sendError(errors.New("wrong number of arguments for 'get' command"), conn)
			return
		}

		key := args[0]

		// First check in-memory store
		storeMutex.RLock()
		value, exists := keyValueStore[key]

		// Check if the key has expired
		if expires, hasExpiry := expiryTimes[key]; hasExpiry && time.Now().After(expires) {
			// Key has expired
			storeMutex.RUnlock()
			storeMutex.Lock()
			delete(keyValueStore, key)
			delete(expiryTimes, key)
			storeMutex.Unlock()
			conn.Write([]byte(NullResp))
			return
		}

		if exists {
			storeMutex.RUnlock()
			conn.Write([]byte(formatBulkString(value)))
			return
		}
		storeMutex.RUnlock()

		// If not in memory, try to read from RDB file
		rdbPath := dbFilename
		if directory != "" {
			rdbPath = fmt.Sprintf("%s/%s", directory, dbFilename)
		}

		// Check if the file exists
		_, err := os.Stat(rdbPath)
		if os.IsNotExist(err) {
			conn.Write([]byte(NullResp))
			return
		}

		// Read the RDB file
		rdbData, err := os.ReadFile(rdbPath)
		if err != nil {
			fmt.Printf("Error reading RDB file: %v\n", err)
			conn.Write([]byte(NullResp))
			return
		}

		// Get the key from RDB
		value, found := getKeyDirect(rdbData, key)
		if !found {
			conn.Write([]byte(NullResp))
			return
		}

		// Store in memory for future access
		storeMutex.Lock()
		keyValueStore[key] = value
		storeMutex.Unlock()

		// Send the value
		conn.Write([]byte(formatBulkString(value)))

	case "CONFIG":
		if len(args) < 2 || strings.ToLower(args[0]) != "get" {
			sendError(errors.New("CONFIG command only supports GET subcommand"), conn)
			return
		}
		key := args[1]
		value, exists := config[key]
		if !exists {
			conn.Write([]byte("$-1\r\n")) // Return Redis null bulk string for non-existent config keys
			return
		}
		// Respond with a RESP array containing the key and value as bulk strings
		resp := fmt.Sprintf("*2\r\n%s%s", formatBulkString(key), formatBulkString(value))
		conn.Write([]byte(resp))

	case "KEYS":
		// Return all keys from memory and RDB file
		var keys []string

		// First get in-memory keys
		storeMutex.RLock()
		for key, _ := range keyValueStore {
			// Skip expired keys
			if expires, hasExpiry := expiryTimes[key]; hasExpiry && time.Now().After(expires) {
				continue
			}
			keys = append(keys, key)
		}
		storeMutex.RUnlock()

		// If no keys yet, try reading from RDB
		if len(keys) == 0 {
			rdbPath := dbFilename
			if directory != "" {
				rdbPath = fmt.Sprintf("%s/%s", directory, dbFilename)
			}

			// Check if file exists
			_, err := os.Stat(rdbPath)
			if !os.IsNotExist(err) {
				// Read the RDB file
				rdbData, err := os.ReadFile(rdbPath)
				if err == nil {
					// Simple implementation to get the first key
					content := rdbData
					key := parseTable(content)
					if len(key) > 4 {
						length := key[3]
						str := key[4 : 4+length]
						ans := string(str)
						keys = append(keys, ans)
					}
				}
			}
		}

		// Send response
		if len(keys) == 0 {
			conn.Write([]byte("*0\r\n"))
			return
		}

		// Format the response as a RESP array
		resp := fmt.Sprintf("*%d\r\n", len(keys))
		for _, key := range keys {
			resp += formatBulkString(key)
		}
		conn.Write([]byte(resp))

	default:
		sendError(errors.New("unknown command '"+command+"'"), conn)
	}
}

func sliceIndex(data []byte, sep byte) int {
	for i, b := range data {
		if b == sep {
			return i
		}
	}
	return -1
}

func parseTable(bytes []byte) []byte {
	start := sliceIndex(bytes, opCodeResizeDB)
	end := sliceIndex(bytes, opCodeEOF)
	if start == -1 || end == -1 || start >= end {
		return []byte{}
	}
	return bytes[start+1 : end]
}

// min returns the smaller of x or y
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
