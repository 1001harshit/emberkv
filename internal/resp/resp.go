// Package resp implements the Redis serialization protocol (RESP2): parsing
// client commands, writing replies, and a small client-side reply parser used
// by tests and the benchmark tool.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	// MaxBulkLen caps a single argument, guarding against hostile lengths.
	MaxBulkLen = 64 << 20
	// MaxArrayLen caps the number of arguments in one command.
	MaxArrayLen = 1 << 20
)

// ErrProtocol is wrapped by every malformed-input error.
var ErrProtocol = errors.New("protocol error")

func protoErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...))
}

// Reader reads RESP data from a connection.
type Reader struct {
	br *bufio.Reader
}

// NewReader returns a Reader with a 64 KiB buffer.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 64<<10)}
}

// Buffered returns the number of bytes already read but not yet consumed.
// A server flushes replies when this reaches zero, which batches responses to
// pipelined commands.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// WaitReadable blocks until at least one byte can be read, without
// consuming it, and returns the read error if the connection fails first.
func (r *Reader) WaitReadable() error {
	_, err := r.br.Peek(1)
	return err
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, protoErr("line too long")
	}
	if err != nil {
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	line = bytes.TrimSuffix(line[:len(line)-1], []byte{'\r'})
	return line, nil
}

func parseInt(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, protoErr("invalid length %q", b)
	}
	return n, nil
}

// ReadCommand reads one command as a list of arguments. It accepts both
// RESP arrays of bulk strings (what clients send) and space-separated inline
// commands (what you type into telnet or nc). An empty result means a blank
// line and should be ignored.
func (r *Reader) ReadCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] != '*' {
		return bytes.Fields(bytes.Clone(line)), nil
	}
	n, err := parseInt(line[1:])
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	if n > MaxArrayLen {
		return nil, protoErr("too many arguments")
	}
	args := make([][]byte, n)
	for i := range args {
		if args[i], err = r.readBulk(); err != nil {
			return nil, err
		}
	}
	return args, nil
}

func (r *Reader) readBulk() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '$' {
		return nil, protoErr("expected '$', got %q", line)
	}
	l, err := parseInt(line[1:])
	if err != nil {
		return nil, err
	}
	if l < 0 || l > MaxBulkLen {
		return nil, protoErr("invalid bulk length")
	}
	buf := make([]byte, l+2)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return nil, err
	}
	if buf[l] != '\r' || buf[l+1] != '\n' {
		return nil, protoErr("bulk string not terminated by CRLF")
	}
	return buf[:l:l], nil
}

// Error is an error reply received from a server.
type Error string

func (e Error) Error() string { return string(e) }

// ReadReply reads one reply and returns it as: string (simple string),
// Error, int64, []byte (bulk string), nil (null) or []any (array).
func (r *Reader) ReadReply() (any, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protoErr("empty reply")
	}
	switch line[0] {
	case '+':
		return string(line[1:]), nil
	case '-':
		return Error(line[1:]), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		l, err := parseInt(line[1:])
		if err != nil || l < 0 {
			return nil, err
		}
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return nil, err
		}
		return buf[:l], nil
	case '*':
		n, err := parseInt(line[1:])
		if err != nil || n < 0 {
			return nil, err
		}
		arr := make([]any, n)
		for i := range arr {
			if arr[i], err = r.ReadReply(); err != nil {
				return nil, err
			}
		}
		return arr, nil
	}
	return nil, protoErr("unknown reply type %q", line[0])
}

// Writer writes RESP data. Replies are buffered until Flush.
type Writer struct {
	bw  *bufio.Writer
	num []byte
}

// NewWriter returns a Writer with a 64 KiB buffer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 64<<10)}
}

func (w *Writer) prefixed(prefix byte, n int64) {
	w.num = strconv.AppendInt(append(w.num[:0], prefix), n, 10)
	w.num = append(w.num, '\r', '\n')
	w.bw.Write(w.num)
}

// WriteSimple writes a simple string such as OK.
func (w *Writer) WriteSimple(s string) {
	w.bw.WriteByte('+')
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// WriteError writes an error reply. msg should start with an error code such
// as "ERR" or "WRONGTYPE".
func (w *Writer) WriteError(msg string) {
	w.bw.WriteByte('-')
	w.bw.WriteString(msg)
	w.bw.WriteString("\r\n")
}

// WriteInt writes an integer reply.
func (w *Writer) WriteInt(n int64) { w.prefixed(':', n) }

// WriteBulk writes a bulk string reply.
func (w *Writer) WriteBulk(b []byte) {
	w.prefixed('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

// WriteBulkString writes a bulk string reply from a string.
func (w *Writer) WriteBulkString(s string) {
	w.prefixed('$', int64(len(s)))
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// WriteNull writes a null bulk string.
func (w *Writer) WriteNull() { w.bw.WriteString("$-1\r\n") }

// WriteNullArray writes a null array, RESP2's "no result" for array replies.
func (w *Writer) WriteNullArray() { w.bw.WriteString("*-1\r\n") }

// WriteArray writes an array header; the caller then writes n elements.
func (w *Writer) WriteArray(n int) { w.prefixed('*', int64(n)) }

// WriteCommand writes a command as an array of bulk strings (client side).
func (w *Writer) WriteCommand(args ...string) {
	w.WriteArray(len(args))
	for _, a := range args {
		w.WriteBulkString(a)
	}
}

// WriteRaw writes already-encoded RESP data.
func (w *Writer) WriteRaw(b []byte) { w.bw.Write(b) }

// Flush sends buffered data.
func (w *Writer) Flush() error { return w.bw.Flush() }
