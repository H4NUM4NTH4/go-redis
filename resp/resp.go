package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Reader wraps a bufio.Reader to read RESP data
type Reader struct {
	reader *bufio.Reader
}

// NewReader creates a new RESP reader
// Think of it like a "smart reader" that understands the Redis language
func NewReader(rd io.Reader) *Reader {
	return &Reader{reader: bufio.NewReader(rd)}
}

// ReadCommand reads one full command from the client
// Returns a slice of strings like ["SET", "name", "John"]
func (r *Reader) ReadCommand() ([]string, error) {
	// Step 1: Read the first byte to know what type is coming
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	// Step 2: Check if it starts with '*' (array)
	if line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %s", string(line[0]))
	}

	// Step 3: Read how many elements are in the array
	count, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %s", line)
	}

	// Step 4: Read each element
	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		arg, err := r.readBulkString()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	return args, nil
}

// readLine reads one line ending with \r\n
// Like reading one sentence that ends with a period
func (r *Reader) readLine() (string, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Strip the \r\n at the end
	return line[:len(line)-2], nil
}

// readBulkString reads a bulk string like:
// $4\r\nname\r\n  →  "name"
func (r *Reader) readBulkString() (string, error) {
	// Read the "$4" part
	line, err := r.readLine()
	if err != nil {
		return "", err
	}

	if line[0] != '$' {
		return "", fmt.Errorf("expected bulk string, got %s", string(line[0]))
	}

	// Get the length (4 in "$4")
	length, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %s", line)
	}

	// Read exactly `length` bytes — the actual string value
	buf := make([]byte, length+2) // +2 for \r\n
	_, err = io.ReadFull(r.reader, buf)
	if err != nil {
		return "", err
	}

	// Return without the trailing \r\n
	return string(buf[:length]), nil
}

// WriteSimpleString writes +OK\r\n back to client
func WriteSimpleString(w io.Writer, s string) {
	w.Write([]byte("+" + s + "\r\n"))
}

// WriteError writes -ERR message\r\n back to client
func WriteError(w io.Writer, msg string) {
	w.Write([]byte("-ERR " + msg + "\r\n"))
}

// WriteNull writes a null response back to client
// Used when a key doesn't exist
func WriteNull(w io.Writer) {
	w.Write([]byte("$-1\r\n"))
}

// WriteBulkString writes $4\r\nname\r\n back to client
func WriteBulkString(w io.Writer, s string) {
	w.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)))
}