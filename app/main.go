package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// RDB file opcodes
const (
	opCodeModuleAux    byte   = 247 // Module auxiliary data
	opCodeIdle         byte   = 248 // LRU idle time
	opCodeFreq         byte   = 249 // LFU frequency
	opCodeAux          byte   = 250 // RDB aux field
	opCodeResizeDB     byte   = 251 // Hash table resize hint
	opCodeExpireTimeMs byte   = 252 // Expire time in milliseconds
	opCodeExpireTime   byte   = 253 // Old expire time in seconds
	opCodeSelectDB     byte   = 254 // DB number of the following keys
	opCodeEOF          byte   = 255 // End of RDB file indicator
	emptyRDB           string = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

// Value type opcodes
const (
	ValueTypeString byte = 0x00
	ValueTypeList   byte = 0x01
	ValueTypeSet    byte = 0x02
	ValueTypeZSet   byte = 0x03
	ValueTypeHash   byte = 0x04
)

// RESP protocol constants
const (
	OKResp   = "+OK\r\n"
	NullResp = "$-1\r\n"
)

// Config holds server configuration
type Config struct {
	Directory     string
	DBFilename    string
	Port          int
	MasterAddress string
	Role          string
	// Replication metadata
	ReplicationID string
	Offset        int
}

// RedisServer represents the Redis server
type RedisServer struct {
	config             Config
	store              *Storage
	listener           net.Listener
	saveSignal         chan struct{}
	replicaConnections map[string]net.Conn // Map to store connections to replicas
	replicasMutex      sync.Mutex          // Mutex to protect access to the connections map
}

// Storage implements the in-memory key-value store
type Storage struct {
	keyValueStore map[string]string
	expiryTimes   map[string]time.Time
	mutex         sync.RWMutex
}

// NewStorage creates a new storage instance
func NewStorage() *Storage {
	return &Storage{
		keyValueStore: make(map[string]string),
		expiryTimes:   make(map[string]time.Time),
	}
}

// Get retrieves a value from storage
func (s *Storage) Get(key string) (string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, exists := s.keyValueStore[key]
	if !exists {
		return "", false
	}

	// Check if the key has expired
	if expiry, hasExpiry := s.expiryTimes[key]; hasExpiry && time.Now().After(expiry) {
		// Key has expired but we're still holding a read lock
		// Return not found, actual cleanup will happen on next write operation
		return "", false
	}

	return value, true
}

// Set stores a value with optional expiry
func (s *Storage) Set(key, value string, expiry *time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.keyValueStore[key] = value
	if expiry != nil {
		s.expiryTimes[key] = *expiry
	} else {
		delete(s.expiryTimes, key)
	}
}

// Delete removes a key from storage
func (s *Storage) Delete(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.keyValueStore, key)
	delete(s.expiryTimes, key)
}

// CleanExpired removes expired keys
func (s *Storage) CleanExpired() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	now := time.Now()
	for key, expiry := range s.expiryTimes {
		if now.After(expiry) {
			delete(s.keyValueStore, key)
			delete(s.expiryTimes, key)
		}
	}
}

// GetAllKeys returns all non-expired keys
func (s *Storage) GetAllKeys() []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var keys []string
	now := time.Now()

	for key := range s.keyValueStore {
		if expiry, hasExpiry := s.expiryTimes[key]; !(hasExpiry && now.After(expiry)) {
			keys = append(keys, key)
		}
	}

	return keys
}

// Scan provides a snapshot of the current data for RDB saving
func (s *Storage) Scan() (map[string]string, map[string]time.Time) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Create copies of the maps to avoid concurrent modification
	keyValues := make(map[string]string, len(s.keyValueStore))
	expiries := make(map[string]time.Time, len(s.expiryTimes))

	for k, v := range s.keyValueStore {
		keyValues[k] = v
	}

	for k, v := range s.expiryTimes {
		expiries[k] = v
	}

	return keyValues, expiries
}

// NewRedisServer creates a new Redis server instance
func NewRedisServer(config Config) (*RedisServer, error) {
	store := NewStorage()

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to bind to port %d: %w", config.Port, err)
	}

	server := &RedisServer{
		config:             config,
		store:              store,
		listener:           listener,
		saveSignal:         make(chan struct{}, 1),
		replicaConnections: make(map[string]net.Conn),
	}

	// Set the global server instance
	setServerInstance(server)

	return server, nil
}

// Start begins the Redis server operation
func (s *RedisServer) Start(ctx context.Context) error {
	log.Printf("Starting Redis server on port %d with RDB file support", s.config.Port)

	// Load data from RDB file if it exists
	if err := s.loadFromRDB(); err != nil {
		log.Printf("Warning: failed to load RDB file: %v", err)
	}

	// Start periodic saving
	go s.periodicSave(ctx)

	// Accept connections in a goroutine
	go s.acceptConnections(ctx)

	// Wait for context cancellation
	<-ctx.Done()
	return s.listener.Close()
}

// acceptConnections handles incoming client connections
func (s *RedisServer) acceptConnections(ctx context.Context) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return // Server is shutting down
			default:
				log.Printf("Error accepting connection: %v", err)
				continue
			}
		}

		// Handle each connection in a separate goroutine
		go s.handleConnection(ctx, conn)
	}
}

// handleConnection processes client commands
func (s *RedisServer) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Monitor for context cancellation
	go func() {
		<-connCtx.Done()
		conn.Close()
	}()

	for {
		command, args, err := parseCommand(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Println("Client disconnected")
				return
			}
			s.sendError(err, conn)
			return
		}
		s.executeCommand(command, args, conn)
	}
}

// periodicSave regularly saves the database to RDB file
func (s *RedisServer) periodicSave(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.saveToRDB(); err != nil {
				log.Printf("Error saving RDB file: %v", err)
			}
		case <-s.saveSignal:
			if err := s.saveToRDB(); err != nil {
				log.Printf("Error saving RDB file: %v", err)
			}
		}
	}
}

// triggerSave signals that a save should occur
func (s *RedisServer) triggerSave() {
	select {
	case s.saveSignal <- struct{}{}:
		// Signal sent
	default:
		// Channel buffer is full, which means a save is already pending
	}
}

// loadFromRDB loads data from the RDB file
func (s *RedisServer) loadFromRDB() error {
	rdbPath := s.getRDBPath()

	// Check if the file exists
	_, err := os.Stat(rdbPath)
	if os.IsNotExist(err) {
		log.Println("RDB file does not exist, starting with empty database")
		return nil
	}

	// Read the RDB file
	rdbData, err := os.ReadFile(rdbPath)
	if err != nil {
		return fmt.Errorf("error reading RDB file: %w", err)
	}

	log.Printf("Loading data from RDB file: %s", rdbPath)
	return s.parseRDBFile(rdbData)
}

// getRDBPath returns the full path to the RDB file
func (s *RedisServer) getRDBPath() string {
	if s.config.Directory == "" {
		return s.config.DBFilename
	}
	return fmt.Sprintf("%s/%s", s.config.Directory, s.config.DBFilename)
}

// parseRDBFile processes the RDB file data
func (s *RedisServer) parseRDBFile(rdbData []byte) error {
	if len(rdbData) < 9 {
		return errors.New("RDB file is too short")
	}

	// Check the REDIS header
	if !bytes.Equal(rdbData[:5], []byte("REDIS")) {
		return errors.New("invalid RDB file: missing REDIS header")
	}

	// Skip past the header and version (9 bytes total)
	pos := 9

	// Variables to track current state
	var pendingExpiry time.Time
	var hasPendingExpiry bool
	var currentDBNum int

	// Process the RDB file until we reach EOF
	for pos < len(rdbData) {
		// Check for EOF marker
		if pos < len(rdbData) && rdbData[pos] == opCodeEOF {
			log.Println("Reached end of RDB file")
			break
		}

		// Process opcodes
		opcode := rdbData[pos]
		pos++

		switch opcode {
		case opCodeSelectDB:
			// Database selector
			if pos < len(rdbData) {
				currentDBNum = int(rdbData[pos])
				pos++
				log.Printf("Switched to DB: %d", currentDBNum)
			}

		case opCodeResizeDB:
			// Skip the resize DB info (hash table sizes)
			var bytesRead int
			_, bytesRead, err := readLength(rdbData, pos)
			if err != nil {
				pos++
				continue
			}
			pos += bytesRead

			_, bytesRead, err = readLength(rdbData, pos)
			if err != nil {
				pos++
				continue
			}
			pos += bytesRead

		case opCodeExpireTime:
			// Read expiry time in seconds (4 bytes)
			if pos+4 > len(rdbData) {
				return errors.New("truncated expire time")
			}

			// Read the 4-byte expiry time (seconds since epoch)
			secondsSinceEpoch := int64(binary.LittleEndian.Uint32(rdbData[pos : pos+4]))
			pendingExpiry = time.Unix(secondsSinceEpoch, 0)
			hasPendingExpiry = true
			pos += 4

		case opCodeExpireTimeMs:
			// Read expiry time in milliseconds (8 bytes)
			if pos+8 > len(rdbData) {
				return errors.New("truncated expire time ms")
			}

			// Read the 8-byte expiry time (milliseconds since epoch)
			millisSinceEpoch := int64(binary.LittleEndian.Uint64(rdbData[pos : pos+8]))
			pendingExpiry = time.Unix(0, millisSinceEpoch*int64(time.Millisecond))
			hasPendingExpiry = true
			pos += 8

		case opCodeAux:
			// Handle auxiliary data (key-value pair, both as strings)
			key, keySize, err := readEncodedString(rdbData, pos)
			if err != nil {
				pos++
				continue
			}
			pos += keySize

			value, valueSize, err := readEncodedString(rdbData, pos)
			if err != nil {
				pos++
				continue
			}
			pos += valueSize

			log.Printf("AUX: %s = %s", key, value)

		case ValueTypeString:
			// Process actual key-value pair
			key, keySize, err := readEncodedString(rdbData, pos)
			if err != nil {
				pos++
				continue
			}
			pos += keySize

			value, valueSize, err := readEncodedString(rdbData, pos)
			if err != nil {
				pos++
				continue
			}
			pos += valueSize

			// Check if the pending expiry time has already passed
			if hasPendingExpiry && time.Now().After(pendingExpiry) {
				// Key has already expired, don't store it
				log.Printf("Skipping expired key '%s' from RDB file", key)
				// Reset pending expiry
				hasPendingExpiry = false
				continue
			}

			// Store in memory
			if hasPendingExpiry {
				expiry := pendingExpiry
				s.store.Set(key, value, &expiry)
				log.Printf("Loaded key '%s' with expiration '%s' from RDB file", key, pendingExpiry)
				hasPendingExpiry = false
			} else {
				s.store.Set(key, value, nil)
				log.Printf("Loaded key '%s' (no expiration) from RDB file", key)
			}

		default:
			// Skip unknown opcodes
			log.Printf("Unknown opcode: %d at position %d", opcode, pos-1)
			pos++
		}
	}

	return nil
}

// saveToRDB saves the current dataset to the RDB file
func (s *RedisServer) saveToRDB() error {
	rdbPath := s.getRDBPath()

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
	// Simple length encoding for small counts
	buf.WriteByte(1)
	buf.WriteByte(0)
	buf.WriteByte(0)

	// Get a snapshot of the current data
	keyValues, expiries := s.store.Scan()

	// Write key-value pairs
	for key, value := range keyValues {
		expiryTime, hasExpiry := expiries[key]

		// Skip expired keys
		if hasExpiry && time.Now().After(expiryTime) {
			continue
		}

		// Write expiry time if present
		if hasExpiry {
			buf.WriteByte(opCodeExpireTimeMs)
			millis := expiryTime.UnixNano() / int64(time.Millisecond)
			binary.Write(&buf, binary.LittleEndian, uint64(millis))
		}

		// String type
		buf.WriteByte(ValueTypeString)

		// Write key
		encodeString(&buf, key)

		// Write value
		encodeString(&buf, value)
	}

	// Write EOF marker
	buf.WriteByte(opCodeEOF)

	// Compute checksum (simple implementation for now - just some bytes)
	checksum := []byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF}
	buf.Write(checksum)

	// Write the RDB file
	if err := os.WriteFile(rdbPath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("error writing RDB file: %w", err)
	}

	log.Printf("Saved %d keys to RDB file: %s", len(keyValues), rdbPath)
	return nil
}

// encodeString writes a string to the buffer using Redis encoding
func encodeString(buf *bytes.Buffer, s string) {
	// Simple implementation - just use length prefixed strings
	// For production, this should implement proper Redis string encoding
	length := len(s)
	if length < 64 {
		// 6-bit length
		buf.WriteByte(byte(length))
	} else {
		// Just an example for larger strings, not complete
		buf.WriteByte(byte(1<<6 | (length>>8)&0x3F))
		buf.WriteByte(byte(length & 0xFF))
	}
	buf.WriteString(s)
}

// sendError sends an error response to the client
func (s *RedisServer) sendError(err error, conn net.Conn) {
	errMsg := fmt.Sprintf("-ERR %s\r\n", err.Error())
	conn.Write([]byte(errMsg))
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (s *RedisServer) propagateCommand(command []string, address string) error {
	fmt.Printf("DEBUG: propagateCommand called for address: %s with command: %v\n", address, command)

	s.replicasMutex.Lock()
	conn, exists := s.replicaConnections[address]
	s.replicasMutex.Unlock()

	if !exists {
		fmt.Printf("DEBUG: No existing connection for %s, attempting to establish one\n", address)
		// If we don't have a connection, try to establish one
		var err error
		conn, err = net.Dial("tcp", address)
		if err != nil {
			fmt.Printf("ERROR: Failed to connect to replica at %s: %v\n", address, err)
			return err
		}
		fmt.Printf("DEBUG: New connection established to %s\n", address)

		// Store the new connection
		s.replicasMutex.Lock()
		s.replicaConnections[address] = conn
		s.replicasMutex.Unlock()
	} else {
		fmt.Printf("DEBUG: Using existing connection for %s\n", address)
	}

	// Format the command as a RESP array
	message := formatRESPArray(command)
	fmt.Printf("DEBUG: Sending message to replica: %q\n", message)

	// Write the command to the replica
	_, err := conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("ERROR: Failed to write to replica at %s: %v\n", address, err)
		// Connection might be dead, remove it
		s.replicasMutex.Lock()
		delete(s.replicaConnections, address)
		s.replicasMutex.Unlock()
		return err
	}

	// Read the response
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			fmt.Printf("ERROR: Connection to replica at %s closed\n", address)
		} else {
			fmt.Printf("ERROR: Failed to read response from replica at %s: %v\n", address, err)
		}
		// Connection might be dead, remove it
		s.replicasMutex.Lock()
		delete(s.replicaConnections, address)
		s.replicasMutex.Unlock()
		return err
	}

	fmt.Printf("DEBUG: Received response from replica at %s: %q\n", address, response)
	return nil
}

// executeRedisCommand is a shared function that executes Redis commands regardless of source
func (s *RedisServer) executeRedisCommand(command string, args []string, conn net.Conn, shouldPropagate bool) {
	log.Printf("Executing command: %s, propagation: %v", command, shouldPropagate)

	switch command {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))

	case "ECHO":
		if len(args) < 1 {
			s.sendError(errors.New("wrong number of arguments for 'echo' command"), conn)
			return
		}
		conn.Write([]byte(formatBulkString(args[0])))

	case "SET":
		if len(args) < 2 {
			s.sendError(errors.New("wrong number of arguments for 'set' command"), conn)
			return
		}

		key := args[0]
		value := args[1]

		// Parse expiry options
		var expiry *time.Time

		if len(args) > 2 && len(args)%2 == 0 {
			for i := 2; i < len(args); i += 2 {
				option := strings.ToLower(args[i])
				if i+1 >= len(args) {
					s.sendError(errors.New("syntax error"), conn)
					return
				}

				val := args[i+1]
				var exp time.Time

				switch option {
				case "ex": // seconds
					seconds, err := strconv.Atoi(val)
					if err != nil {
						s.sendError(errors.New("invalid expire time"), conn)
						return
					}
					exp = time.Now().Add(time.Duration(seconds) * time.Second)
					expiry = &exp

				case "px": // milliseconds
					millis, err := strconv.Atoi(val)
					if err != nil {
						s.sendError(errors.New("invalid expire time"), conn)
						return
					}
					exp = time.Now().Add(time.Duration(millis) * time.Millisecond)
					expiry = &exp
				}
			}
		}

		// Store the key-value pair
		s.store.Set(key, value, expiry)

		// Trigger a save
		s.triggerSave()

		if conn != nil {
			conn.Write([]byte(OKResp))
		}

		// Propagate to replicas if needed
		if shouldPropagate && s.config.Role == "master" {
			s.replicasMutex.Lock()
			replicaCount := len(s.replicaConnections)
			s.replicasMutex.Unlock()

			if replicaCount > 0 {
				fmt.Println("DEBUG: Propagating SET command to replicas")
				// Combine command and arguments back together
				fullCommand := append([]string{"SET"}, args...)

				// Iterate over all replica connections
				s.replicasMutex.Lock()
				for address := range s.replicaConnections {
					go s.propagateCommand(fullCommand, address)
				}
				s.replicasMutex.Unlock()
			}
		}

	case "GET":
		if len(args) < 1 {
			s.sendError(errors.New("wrong number of arguments for 'get' command"), conn)
			return
		}

		key := args[0]
		value, exists := s.store.Get(key)

		if !exists {
			conn.Write([]byte(NullResp))
			return
		}

		conn.Write([]byte(formatBulkString(value)))

	case "DEL":
		if len(args) < 1 {
			s.sendError(errors.New("wrong number of arguments for 'del' command"), conn)
			return
		}

		count := 0
		for _, key := range args {
			if _, exists := s.store.Get(key); exists {
				s.store.Delete(key)
				count++
			}
		}

		// Trigger a save
		s.triggerSave()

		// Send response
		if conn != nil {
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))
		}

		// Propagate to replicas if needed
		if shouldPropagate && s.config.Role == "master" {
			s.replicasMutex.Lock()
			replicaCount := len(s.replicaConnections)
			s.replicasMutex.Unlock()

			if replicaCount > 0 {
				fmt.Println("DEBUG: Propagating DEL command to replicas")
				fullCommand := append([]string{"DEL"}, args...)

				s.replicasMutex.Lock()
				for address := range s.replicaConnections {
					go s.propagateCommand(fullCommand, address)
				}
				s.replicasMutex.Unlock()
			}
		}

	case "CONFIG":
		if len(args) < 2 || strings.ToLower(args[0]) != "get" {
			s.sendError(errors.New("CONFIG command only supports GET subcommand"), conn)
			return
		}

		key := args[1]
		var value string
		var exists bool

		// Map config key to actual value
		switch key {
		case "dir":
			value = s.config.Directory
			exists = s.config.Directory != ""
		case "dbfilename":
			value = s.config.DBFilename
			exists = true
		default:
			exists = false
		}

		if !exists {
			conn.Write([]byte("$-1\r\n"))
			return
		}

		// Respond with a RESP array containing the key and value as bulk strings
		resp := fmt.Sprintf("*2\r\n%s%s", formatBulkString(key), formatBulkString(value))
		conn.Write([]byte(resp))

	case "KEYS":
		keys := s.store.GetAllKeys()

		// Send response
		if len(keys) == 0 {
			conn.Write([]byte("*0\r\n"))
			return
		}

		conn.Write([]byte(formatRESPArray(keys)))

	case "INFO":
		// Get INFO argument
		// At this stage, we only support "replication" key
		if len(args) < 1 {
			s.sendError(errors.New("wrong number of arguments for 'info' command"), conn)
			return
		}

		// replication keys
		conn.Write([]byte(formatBulkString(fmt.Sprintf("role:%s master_replid:%s master_repl_offset:%d", s.config.Role, s.config.ReplicationID, s.config.Offset))))

	case "REPLCONF":
		if len(args) < 2 {
			s.sendError(errors.New("wrong number of arguments for 'replconf' command"), conn)
			return
		}

		if args[0] == "listening-port" {
			// Store the connection that's currently being used
			address := fmt.Sprintf("localhost:%s", args[1])
			fmt.Printf("DEBUG: Registering replica at address: %s\n", address)
			s.replicasMutex.Lock()
			s.replicaConnections[address] = conn
			fmt.Printf("DEBUG: After registration, replica count: %d\n", len(s.replicaConnections))
			s.replicasMutex.Unlock()
		} else {
			fmt.Printf("DEBUG: REPLCONF with argument: %s (not listening-port)\n", args[0])
		}
		conn.Write([]byte(OKResp))

	case "PSYNC":
		data, err := hex.DecodeString(emptyRDB)
		if err != nil {
			log.Fatalf("Failed to decode hex string: %v", err)
		}

		conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", s.config.ReplicationID, s.config.Offset)))
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(data), data)))

	case "SELECT":
		// In our implementation, we ignore SELECT commands since we only support a single database
		if conn != nil {
			conn.Write([]byte(OKResp))
		}

	default:
		if conn != nil {
			s.sendError(errors.New("unknown command '"+command+"'"), conn)
		} else {
			fmt.Printf("DEBUG: Ignoring unknown command: %s from master\n", command)
		}
	}
}

// executeCommand processes a Redis command from a client
func (s *RedisServer) executeCommand(command string, args []string, conn net.Conn) {
	// Call the shared execute function with propagation enabled
	s.executeRedisCommand(command, args, conn, true)
}

// processMasterCommands continuously reads and processes commands from the master
func processMasterCommands(conn net.Conn) {
	reader := bufio.NewReader(conn)

	for {
		// First message after PSYNC will be the RDB file, we need to read and process it
		bulkStringHeader, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Master connection closed")
				return
			}
			fmt.Printf("Error reading from master: %v\n", err)
			return
		}

		// If it's an RDB file transfer (starts with $)
		if strings.HasPrefix(bulkStringHeader, "$") {
			fmt.Println("DEBUG: Received RDB file header from master")

			// Skip RDB file processing for now
			bulkStringHeaderPattern := regexp.MustCompile(`^\$(\d+)\r\n$`)
			matches := bulkStringHeaderPattern.FindStringSubmatch(bulkStringHeader)
			if len(matches) < 2 {
				fmt.Println("Invalid RDB header format")
				continue
			}

			count, err := strconv.Atoi(matches[1])
			if err != nil || count < 0 {
				fmt.Println("Invalid RDB length")
				continue
			}

			// Read and discard the RDB data
			rdbData := make([]byte, count+2) // +2 for \r\n
			_, err = io.ReadFull(reader, rdbData)
			if err != nil {
				fmt.Printf("Error reading RDB data: %v\n", err)
				return
			}

			fmt.Printf("DEBUG: Successfully read %d bytes of RDB data\n", count)
			continue
		}

		// Regular command processing for replication
		if strings.HasPrefix(bulkStringHeader, "*") {
			fmt.Println("DEBUG: Received command from master")

			// Parse array header to get number of elements
			count, valid := parseArrayHeader(bulkStringHeader)
			if !valid {
				fmt.Println("Invalid array header from master")
				continue
			}

			// Read all elements (command + args)
			var command string
			var arguments []string

			for i := 0; i < count; i++ {
				element, err := parseBulkString(reader)
				if err != nil {
					fmt.Printf("Error parsing element: %v\n", err)
					break
				}

				if i == 0 {
					command = strings.ToUpper(element)
				} else {
					arguments = append(arguments, element)
				}
			}

			// Get server instance
			server := getServerInstance()
			if server == nil {
				fmt.Println("ERROR: Could not access server instance")
				continue
			}

			// Execute the command without propagation (since it's from master)
			server.executeRedisCommand(command, arguments, nil, false)
		}
	}
}

// Global server instance for access from command processing
var globalServer *RedisServer
var serverMutex sync.Mutex

// setServerInstance safely sets the global server instance
func setServerInstance(server *RedisServer) {
	serverMutex.Lock()
	defer serverMutex.Unlock()
	globalServer = server
}

// getServerInstance safely retrieves the global server instance
func getServerInstance() *RedisServer {
	serverMutex.Lock()
	defer serverMutex.Unlock()
	return globalServer
}

func main() {
	// Parse command line flags
	config := Config{}

	flag.StringVar(&config.Directory, "dir", "", "Directory for files")
	flag.StringVar(&config.DBFilename, "dbfilename", "dump.rdb", "Filename for the RDB database file")
	flag.IntVar(&config.Port, "port", 6379, "Application port")
	flag.StringVar(&config.MasterAddress, "replicaof", "", "Run in replica mode. Address of master node.")
	flag.Parse()

	// Set role and initialize replication ID
	config.ReplicationID = randSeq(40)
	config.Offset = 0

	if config.MasterAddress == "" {
		config.Role = "master"
	} else {
		config.Role = "slave"
	}

	// Create the server instance
	server, err := NewRedisServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// If we're a replica, establish connection to master
	if config.Role == "slave" {
		fmt.Printf("DEBUG: Running as replica, connecting to master at %s\n", config.MasterAddress)

		masterConn, err := replicaHandshake(config.MasterAddress, config.Port)
		if err != nil {
			log.Printf("Warning: Failed to establish connection to master: %v", err)
		} else {
			// Store the master connection with the address as key
			parts := strings.Split(config.MasterAddress, " ")
			masterAddress := fmt.Sprintf("%s:%s", parts[0], parts[1])

			fmt.Printf("DEBUG: Storing master connection with key: %s\n", masterAddress)
			server.replicasMutex.Lock()
			server.replicaConnections[masterAddress] = masterConn
			fmt.Printf("DEBUG: Connection map now has %d entries\n", len(server.replicaConnections))
			server.replicasMutex.Unlock()

			log.Printf("Successfully connected to master at %s", masterAddress)
		}
	} else {
		fmt.Println("DEBUG: Running as master, waiting for replica connections")
	}

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	// Start the server
	if err := server.Start(ctx); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func formatRESPArray(arr []string) string {
	resp := fmt.Sprintf("*%d\r\n", len(arr))
	for _, key := range arr {
		resp += formatBulkString(key)
	}
	return resp
}

// formatBulkString formats a string as a RESP bulk string
func formatBulkString(text string) string {
	if text == "" {
		return NullResp
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(text), text)
}

// parseCommand parses a RESP protocol command
func parseCommand(reader *bufio.Reader) (string, []string, error) {
	arrayHeader, err := reader.ReadString('\n')
	if err != nil {
		return "", nil, err
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

// parseBulkString parses a RESP protocol bulk string
func parseBulkString(reader *bufio.Reader) (string, error) {
	bulkStringHeader, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	// Check for null bulk string
	if bulkStringHeader == "$-1\r\n" {
		return "", nil
	}

	// Parse length
	bulkStringHeaderPattern := regexp.MustCompile(`^\$(\d+)\r\n$`)
	matches := bulkStringHeaderPattern.FindStringSubmatch(bulkStringHeader)
	if len(matches) < 2 {
		return "", errors.New("invalid bulk string header format")
	}

	count, err := strconv.Atoi(matches[1])
	if err != nil || count < 0 {
		return "", errors.New("invalid bulk string length")
	}

	// Read exactly count bytes plus \r\n
	bulkString := make([]byte, count+2) // +2 for \r\n
	_, err = io.ReadFull(reader, bulkString)
	if err != nil {
		return "", err
	}

	// Verify the last two bytes are \r\n
	if !bytes.Equal(bulkString[count:], []byte("\r\n")) {
		return "", errors.New("bulk string missing terminator")
	}

	return string(bulkString[:count]), nil
}

// parseArrayHeader parses a RESP protocol array header
func parseArrayHeader(line string) (int, bool) {
	pattern := regexp.MustCompile(`^\*(\d+)\r\n$`)
	matches := pattern.FindStringSubmatch(line)

	if len(matches) < 2 {
		return 0, false
	}

	count, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, false
	}

	return count, true
}

// readEncodedString reads a Redis encoded string from the RDB file.
func readEncodedString(data []byte, offset int) (string, int, error) {
	if offset >= len(data) {
		return "", 0, io.EOF
	}

	// Get the first byte to determine the encoding
	firstByte := data[offset]

	// Check if this is a length-prefixed string or a special encoding
	switch (firstByte & 0xC0) >> 6 {
	case 0: // 00 prefix - 6 bit length string
		length := int(firstByte & 0x3F)
		offset++

		if offset+length > len(data) {
			return "", 1, io.ErrUnexpectedEOF
		}

		return string(data[offset : offset+length]), length + 1, nil

	case 1: // 01 prefix - 14 bit length string
		if offset+1 >= len(data) {
			return "", 1, io.ErrUnexpectedEOF
		}

		length := (int(firstByte&0x3F) << 8) | int(data[offset+1])
		offset += 2

		if offset+length > len(data) {
			return "", 2, io.ErrUnexpectedEOF
		}

		return string(data[offset : offset+length]), length + 2, nil

	case 2: // 10 prefix - 32 bit length string
		if offset+4 >= len(data) {
			return "", 1, io.ErrUnexpectedEOF
		}

		length := binary.BigEndian.Uint32(data[offset+1 : offset+5])
		offset += 5

		if offset+int(length) > len(data) {
			return "", 5, io.ErrUnexpectedEOF
		}

		return string(data[offset : offset+int(length)]), int(length) + 5, nil

	case 3: // 11 prefix - Special encoding
		specialType := firstByte & 0x3F

		switch specialType {
		case 0: // 8 bit integer
			if offset+1 >= len(data) {
				return "", 1, io.ErrUnexpectedEOF
			}

			value := int(data[offset+1])
			return strconv.Itoa(value), 2, nil

		case 1: // 16 bit integer
			if offset+2 >= len(data) {
				return "", 1, io.ErrUnexpectedEOF
			}

			value := int(binary.LittleEndian.Uint16(data[offset+1 : offset+3]))
			return strconv.Itoa(value), 3, nil

		case 2: // 32 bit integer
			if offset+4 >= len(data) {
				return "", 1, io.ErrUnexpectedEOF
			}

			value := int(binary.LittleEndian.Uint32(data[offset+1 : offset+5]))
			return strconv.Itoa(value), 5, nil

		default:
			return "", 1, fmt.Errorf("unsupported special encoding format %d", specialType)
		}
	}

	return "", 0, fmt.Errorf("invalid string encoding: %x", firstByte)
}

// readLength reads a length-encoded integer from the byte slice.
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

func replicaHandshake(masterAddress string, replicaPort int) (net.Conn, error) {
	addressParts := strings.Split(masterAddress, " ")
	fmt.Printf("DEBUG: Initiating handshake with master at %s:%s\n", addressParts[0], addressParts[1])

	// 1. Send PING
	message := formatRESPArray([]string{"PING"})
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", addressParts[0], addressParts[1]))

	if err != nil {
		fmt.Println("Error connecting to host:", err)
		return nil, err
	}
	fmt.Println("DEBUG: Successfully connected to master")

	conn.Write([]byte(message))

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response:", err)
		return nil, err
	}

	if response != "+PONG\r\n" {
		return nil, errors.New("Did not received expected response PONG")
	}

	// 2. Send REPLCONF

	// 2(a). Send 'REPLCONF listening-port'
	conn.Write([]byte(formatRESPArray([]string{"REPLCONF", "listening-port", strconv.Itoa(replicaPort)})))
	response, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response:", err)
		return nil, err
	}

	if response != "+OK\r\n" {
		return nil, err
	}
	// 2(a). Send 'REPLCONF capa'
	conn.Write([]byte(formatRESPArray([]string{"REPLCONF", "capa", "psync2"})))
	response, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response:", err)
		return nil, err
	}

	if response != "+OK\r\n" {
		return nil, err
	}

	// 3. Send PSYNC
	// PSYNC <replicationID> <offset>
	conn.Write([]byte(formatRESPArray([]string{"PSYNC", "?", "-1"})))
	response, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading response:", err)
		return nil, err
	}

	if !strings.Contains(response, "FULLRESYNC") {
		return nil, err
	}

	// Start a goroutine to process commands from the master
	go processMasterCommands(conn)

	return conn, nil
}
