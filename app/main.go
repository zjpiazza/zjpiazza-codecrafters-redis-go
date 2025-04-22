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
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1
var _ = net.Listen
var _ = os.Exit

// config is kept global for simplicity
var config = make(map[string]string)

// currentDB keeps track of the current database number
var currentDB int

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

// readString reads a length-encoded string from the byte slice
func readString(data []byte, offset int) (string, int, error) {
	if offset >= len(data) {
		return "", 0, io.EOF
	}

	length, lengthBytes, err := readLength(data, offset)
	if err != nil {
		return "", 0, fmt.Errorf("failed to read string length at offset %d: %w", offset, err)
	}

	stringOffset := offset + lengthBytes
	if stringOffset+length > len(data) {
		return "", 0, io.ErrUnexpectedEOF
	}

	str := string(data[stringOffset : stringOffset+length])
	return str, lengthBytes + length, nil
}

// Get value for a specific key from an RDB file
// Returns the value as a byte slice and a boolean indicating if the key was found
func readKeyValuePair(rdbData []byte, targetKey string) ([]byte, bool, error) {
	offset := 0

	// Skip header (REDIS and version)
	// Magic string "REDIS" (5 bytes) + Version (4 bytes) = 9 bytes
	if len(rdbData) < 9 {
		return nil, false, fmt.Errorf("file too short to contain RDB header")
	}

	// Basic validation: Check for "REDIS" magic string
	if !bytes.Equal(rdbData[offset:offset+5], []byte("REDIS")) {
		return nil, false, fmt.Errorf("invalid RDB magic string")
	}
	offset += 9 // Skip header

	for offset < len(rdbData) {
		if offset >= len(rdbData) {
			return nil, false, fmt.Errorf("unexpected end of file at offset %d", offset)
		}

		opcode := rdbData[offset]
		offset++

		switch opcode {
		case opCodeAux:
			// Skip auxiliary key
			keyLen, keyBytes, err := readLength(rdbData, offset)
			if err != nil {
				return nil, false, fmt.Errorf("failed to read auxiliary key length at offset %d: %w", offset, err)
			}
			offset += keyBytes + keyLen

			// Skip auxiliary value
			valueLen, valueBytes, err := readLength(rdbData, offset)
			if err != nil {
				return nil, false, fmt.Errorf("failed to read auxiliary value length at offset %d: %w", offset, err)
			}
			offset += valueBytes + valueLen

		case opCodeSelectDB:
			// Read database number
			dbNum, dbNumBytes, err := readLength(rdbData, offset)
			if err != nil {
				return nil, false, fmt.Errorf("failed to read database number at offset %d: %w", offset, err)
			}
			offset += dbNumBytes
			currentDB = dbNum

		case opCodeResizeDB:
			// Skip database size info
			_, hashTableBytes, err := readLength(rdbData, offset)
			if err != nil {
				return nil, false, fmt.Errorf("failed to read hash table size at offset %d: %w", offset, err)
			}
			offset += hashTableBytes

			_, expiryHashTableBytes, err := readLength(rdbData, offset)
			if err != nil {
				return nil, false, fmt.Errorf("failed to read expiry hash table size at offset %d: %w", offset, err)
			}
			offset += expiryHashTableBytes

		case opCodeExpireTime:
			// Skip expiry time (4 bytes)
			offset += 4
			// Continue to value type

		case opCodeExpireTimeMs:
			// Skip expiry time (8 bytes)
			offset += 8
			// Continue to value type

		case opCodeEOF:
			// End of file
			return nil, false, nil

		default:
			// This should be a value type
			valueType := opcode

			// Read key
			keyLen, keyLenBytes, err := readLength(rdbData, offset)
			if err != nil {
				return nil, false, fmt.Errorf("failed to read key length at offset %d: %w", offset, err)
			}
			offset += keyLenBytes

			if offset+keyLen > len(rdbData) {
				return nil, false, fmt.Errorf("key extends beyond end of file at offset %d", offset)
			}

			key := string(rdbData[offset : offset+keyLen])
			offset += keyLen

			// Check if this is our target key
			if key == targetKey {
				// Found our target key, now read the value
				if valueType == ValueTypeString {
					valueLen, valueLenBytes, err := readLength(rdbData, offset)
					if err != nil {
						return nil, false, fmt.Errorf("failed to read value length at offset %d: %w", offset, err)
					}
					offset += valueLenBytes

					if offset+valueLen > len(rdbData) {
						return nil, false, fmt.Errorf("value extends beyond end of file at offset %d", offset)
					}

					value := rdbData[offset : offset+valueLen]
					return value, true, nil
				} else {
					return nil, false, fmt.Errorf("unsupported value type %d for key %s", valueType, key)
				}
			} else {
				// Not our target key, skip the value
				if valueType == ValueTypeString {
					valueLen, valueLenBytes, err := readLength(rdbData, offset)
					if err != nil {
						return nil, false, fmt.Errorf("failed to read value length at offset %d: %w", offset, err)
					}
					offset += valueLenBytes + valueLen
				} else {
					// For now, we don't handle other value types
					return nil, false, fmt.Errorf("unsupported value type %d for key %s", valueType, key)
				}
			}
		}
	}

	return nil, false, fmt.Errorf("key %s not found in RDB file", targetKey)
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
		sendError(errors.New("SET command not implemented for RDB mode"), conn)
		return

	case "GET":
		if len(args) < 1 {
			sendError(errors.New("wrong number of arguments for 'get' command"), conn)
			return
		}

		// Form the full path to the RDB file
		rdbPath := dbFilename
		if directory != "" {
			rdbPath = fmt.Sprintf("%s/%s", directory, dbFilename)
		}

		// Read the RDB file
		rdbData, err := os.ReadFile(rdbPath)
		if err != nil {
			sendError(fmt.Errorf("failed to read RDB file: %w", err), conn)
			return
		}

		// Get the key from RDB
		key := args[0]
		value, found, err := readKeyValuePair(rdbData, key)
		if err != nil {
			fmt.Printf("Error finding key '%s': %v\n", key, err)
			conn.Write([]byte(NullResp)) // Return null for errors
			return
		}

		if !found {
			conn.Write([]byte(NullResp)) // Return null for non-existent keys
			return
		}

		// Send the value
		conn.Write([]byte(formatBulkString(string(value))))

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
		// Return all keys from the RDB file
		rdbPath := dbFilename
		if directory != "" {
			rdbPath = fmt.Sprintf("%s/%s", directory, dbFilename)
		}

		// Simple implementation to just return a "*1\r\n" response with the first key found
		// This would need to be expanded to return all keys in a real implementation
		rdbData, err := os.ReadFile(rdbPath)
		if err != nil {
			sendError(fmt.Errorf("failed to read RDB file: %w", err), conn)
			return
		}

		// Simplified version that uses your original code
		content := rdbData
		key := parseTable(content)
		length := key[3]
		str := key[4 : 4+length]
		ans := string(str)
		conn.Write([]byte(fmt.Sprintf("*1\r\n$%v\r\n%s\r\n", len(ans), ans)))

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
	return bytes[start+1 : end]
}
