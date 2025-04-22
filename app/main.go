package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// db and config are now global for simplicity in this example,
// but consider passing them to functions in a larger application.
var db = make(map[string]KeystoreEntry)
var config = make(map[string]string)

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
	opCodeEOF          byte = 255
)

// Read arguments
var directory string
var dbFilename string

func main() {
	fmt.Println("Logs from your program will appear here!")

	flag.StringVar(&directory, "dir", "", "Directory for files")                           // directory flag is not used in this updated code
	flag.StringVar(&dbFilename, "dbfilename", "db.json", "Filename for the database file") // Default to db.json
	flag.Parse()

	configFilename := "config.json" // Assuming a fixed config filename for now

	// Create file readers/writers
	dbReader := &JSONFileReader{filename: dbFilename}
	dbWriter := &JSONFileWriter{filename: dbFilename}
	configReader := &JSONFileReader{filename: configFilename}

	// Load db and config using the interfaces
	var loadDBErr error
	db, loadDBErr = loadDatabase(dbReader)
	if loadDBErr != nil {
		fmt.Printf("Error loading database from %s: %v. Starting with an empty database.\n", dbFilename, loadDBErr)
		db = make(map[string]KeystoreEntry) // Initialize with an empty map on error
	} else {
		fmt.Printf("Database loaded successfully from %s.\n", dbFilename)
	}

	var loadConfigErr error
	config, loadConfigErr = loadConfig(configReader)
	if loadConfigErr != nil {
		fmt.Printf("Error loading config from %s: %v. Starting with empty config.\n", configFilename, loadConfigErr)
		config = make(map[string]string) // Initialize with an empty map on error
		if directory != "" {
			config["dir"] = directory
		}
		if dbFilename != "" {
			config["dbfilename"] = dbFilename
		}
	} else {
		fmt.Printf("Config loaded successfully from %s.\n", configFilename)
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

		go handleConnection(conn, dbWriter) // Pass the writer to handlers that might save
	}
}

func handleConnection(conn net.Conn, dbWriter JSONWriter) {
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
		executeCommand(command, args, conn, dbWriter) // Pass the writer
	}
}

// JSONReader defines the interface for reading data into a map from a source.
type JSONReader interface {
	Read(target map[string]interface{}) error
}

// JSONWriter defines the interface for writing data from a map to a destination.
type JSONWriter interface {
	Write(data map[string]interface{}) error
}

// JSONFileReader implements JSONReader for file reading.
type JSONFileReader struct {
	filename string
}

func (r *JSONFileReader) Read(target map[string]interface{}) error {
	// Read file
	jsonData, err := os.ReadFile(r.filename)
	if err != nil {
		return err // Return the original error, including os.ErrNotExist
	}

	// If the file is empty, return an empty map without unmarshalling
	if len(jsonData) == 0 {
		for k := range target { // Clear existing map if target was not empty
			delete(target, k)
		}
		return nil
	}

	// Unmarshal JSON to map
	return json.Unmarshal(jsonData, &target)
}

// JSONFileWriter implements JSONWriter for file writing.
type JSONFileWriter struct {
	filename string
}

func (w *JSONFileWriter) Write(data map[string]interface{}) error {
	// Marshal the map to JSON
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	// Write JSON to file
	// Use a temporary file and rename to ensure atomicity for writes
	tmpFilename := w.filename + ".tmp"
	err = os.WriteFile(tmpFilename, jsonData, 0644)
	if err != nil {
		return err
	}

	// Rename the temporary file to the final filename
	err = os.Rename(tmpFilename, w.filename)
	if err != nil {
		// Clean up the temporary file if renaming fails
		os.Remove(tmpFilename)
		return err
	}

	return nil
}

func loadDatabase(reader JSONReader) (map[string]KeystoreEntry, error) {
	// Use map[string]interface{} for reading as json.Unmarshal populates this.
	genericDB := make(map[string]interface{})
	err := reader.Read(genericDB)
	if err != nil {
		// Return the error for the caller to handle (e.g., os.ErrNotExist)
		return nil, err
	}

	// Convert the loaded map[string]interface{} to map[string]KeystoreEntry
	convertedDB := make(map[string]KeystoreEntry)
	for key, value := range genericDB {
		entryMap, ok := value.(map[string]interface{})
		if !ok {
			fmt.Printf("Warning: Database entry for key '%s' is not an expected map format. Skipping.\n", key)
			continue
		}

		var keystoreEntry KeystoreEntry
		// Assuming 'value' and 'expiry' are fields in the JSON object
		if val, valOk := entryMap["value"]; valOk {
			keystoreEntry.value = val // Store the value as interface{}
		}

		if exp, expOk := entryMap["expiry"]; expOk {
			// Assuming expiry is stored as a string formatted with time.RFC3339
			expStr, isString := exp.(string)
			if isString {
				parsedTime, parseErr := time.Parse(time.RFC3339, expStr)
				if parseErr == nil {
					keystoreEntry.expiry = parsedTime
				} else {
					fmt.Printf("Warning: Failed to parse expiry for key '%s': %v. Treating as no expiry.\n", key, parseErr)
				}
			} else if exp != nil { // Handle cases where 'expiry' might be explicitly null in JSON
				fmt.Printf("Warning: Expiry for key '%s' is not a string. Treating as no expiry.\n", key)
			}
		}
		convertedDB[key] = keystoreEntry
	}

	return convertedDB, nil
}

func saveDatabase(writer JSONWriter, db map[string]KeystoreEntry) error {
	// Convert the map[string]KeystoreEntry to map[string]interface{} for writing
	dataToSave := make(map[string]interface{})
	for key, entry := range db {
		entryMap := make(map[string]interface{})
		entryMap["value"] = entry.value // Store the value

		// Handle saving expiry time
		if expiryTime, ok := entry.expiry.(time.Time); ok {
			entryMap["expiry"] = expiryTime.Format(time.RFC3339) // Save as RFC3339 formatted string
		} else {
			entryMap["expiry"] = nil // Save as null if no expiry
		}
		dataToSave[key] = entryMap
	}
	return writer.Write(dataToSave)
}

func loadConfig(reader JSONReader) (map[string]string, error) {
	// Use map[string]interface{} for reading as json.Unmarshal populates this.
	genericConfig := make(map[string]interface{})
	err := reader.Read(genericConfig)
	if err != nil {
		// Return the error for the caller to handle (e.g., os.ErrNotExist)
		return nil, err
	}

	// Convert the loaded map[string]interface{} to map[string]string
	convertedConf := make(map[string]string)
	for key, value := range genericConfig {
		strValue, ok := value.(string)
		if !ok {
			// Log a warning but continue processing other config entries
			fmt.Printf("Warning: Config value for key '%s' is not a string. Skipping.\n", key)
			continue
		}
		convertedConf[key] = strValue
	}
	return convertedConf, nil
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
		element, err := parseBulkString(reader) // parseBulkString no longer needs the connection
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

type KeystoreEntry struct {
	value  interface{} // Using interface{} to store any type of value
	expiry interface{} // Using interface{} to store time.Time or nil
}

func executeCommand(command string, args []string, conn net.Conn, dbWriter JSONWriter) {
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

		var expiry interface{} // Use interface{}

		// Check for optional arguments (PX)
		if len(args) > 2 && strings.ToLower(args[2]) == "nx" {
			// SET with NX option: set the key only if it does not already exist.
			_, exists := db[key]
			if exists {
				// According to Redis SET NX behavior, return null bulk string if key exists.
				conn.Write([]byte("$-1\r\n"))
				return
			}
			// Continue with setting the key if not exists.
			if len(args) > 4 && strings.ToLower(args[3]) == "px" {
				// Handle PX after NX
				expMilli, err := strconv.Atoi(args[4])
				if err != nil {
					sendError(errors.New("expiration time is not a valid integer for PX option"), conn)
					return
				}
				expiry = time.Now().Add(time.Duration(expMilli) * time.Millisecond)
			}

		} else if len(args) > 2 && strings.ToLower(args[2]) == "xx" {
			// SET with XX option: set the key only if it already exists.
			_, exists := db[key]
			if !exists {
				// According to Redis SET XX behavior, return null bulk string if key does not exist.
				conn.Write([]byte("$-1\r\n"))
				return
			}
			// Continue with setting the key if it exists.
			if len(args) > 4 && strings.ToLower(args[3]) == "px" {
				// Handle PX after XX
				expMilli, err := strconv.Atoi(args[4])
				if err != nil {
					sendError(errors.New("expiration time is not a valid integer for PX option"), conn)
					return
				}
				expiry = time.Now().Add(time.Duration(expMilli) * time.Millisecond)
			}

		} else if len(args) > 2 && strings.ToLower(args[2]) == "px" {
			// SET with PX option
			if len(args) < 4 {
				sendError(errors.New("PX option requires an expiration time"), conn)
				return
			}
			expMilli, err := strconv.Atoi(args[3])
			if err != nil {
				sendError(errors.New("expiration time is not a valid integer for PX option"), conn)
				return
			}
			expiry = time.Now().Add(time.Duration(expMilli) * time.Millisecond)
		}

		db[key] = KeystoreEntry{value, expiry}

		// Save the database after a successful SET
		err := saveDatabase(dbWriter, db)
		if err != nil {
			fmt.Println("Error writing to database:", err)
			// Consider sending an error back to the client here as well
		}
		conn.Write([]byte("+OK\r\n"))

	case "GET":
		if len(args) < 1 {
			sendError(errors.New("wrong number of arguments for 'get' command"), conn)
			return
		}
		key := args[0]
		entry, exists := db[key]

		if !exists {
			conn.Write([]byte("$-1\r\n")) // Return Redis null bulk string for non-existent keys
			return
		}

		// Check for expiry if it exists and is a time.Time
		if expiryTime, ok := entry.expiry.(time.Time); ok {
			if time.Now().After(expiryTime) {
				delete(db, key) // Remove the expired key
				// Optionally, save the database here or rely on periodic saves
				conn.Write([]byte("$-1\r\n")) // Return null bulk string for expired keys
				return
			}
		}

		// Safely get the value as a string
		strValue, ok := entry.value.(string)
		if !ok {
			// Handle cases where the stored value is not a string (shouldn't happen with current SET)
			fmt.Printf("Warning: Value for key '%s' is not a string. Returning null.\n", key)
			conn.Write([]byte("$-1\r\n"))
			return
		}

		conn.Write([]byte(formatBulkString(strValue)))

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
		content, _ := os.ReadFile(fmt.Sprintf("%s/%s", directory, dbFilename))
		key := parseTable(content)
		length := key[3]
		str := key[4 : 4+length]
		ans := string(str)
		conn.Write([]byte(fmt.Sprintf("*1\r\n$%v\r\n%s\r\n", len(ans), ans)))
		return
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
