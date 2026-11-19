package grpc_net_conn

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Conn implements net.Conn across a gRPC stream. You must populate many
// of the exported structs on this field so please read the documentation.
//
// There are a number of limitations to this implementation, typically due
// limitations of visibility given by the gRPC stream. Deadlines do not work.
// Use keepalive to detect a dead stream instead.
//
// The connection ends when a Read fails, a Write fails with an error other
// than io.EOF, or Close is called. A Write gets io.EOF when a client stream
// has ended, and a Read then returns the unread data and the actual error.
// Done and Err report when and why the connection ended. Reads and writes
// after that return the same error.
//
// As documented on net.Conn, it is safe for concurrent read/write.
type Conn struct {
	// Stream is the stream to wrap into a Conn. This can be either a client
	// or server stream and we will perform correctly.
	Stream grpc.Stream

	// Request is the type to use for sending request data to the streaming
	// endpoint. This must be a non-nil allocated value and must NOT point to
	// the same value as Response since they may be used concurrently.
	//
	// The Reset method is never called on Request so you may set some
	// fields on the request type and they will be sent for every request
	// unless the Encode field changes it.
	Request proto.Message

	// Response is the type to use for reading response data. This must be
	// a non-nil allocated value and must NOT point to the same value as Request
	// since they may be used concurrently.
	//
	// The Reset method will be called on Response during Reads so data you
	// set initially will be lost.
	Response proto.Message

	// ResponseLock, if non-nil, will be locked while calling SendMsg
	// on the Stream. This can be used to prevent concurrent access to
	// SendMsg which is unsafe.
	ResponseLock *sync.Mutex

	// Encode encodes messages into the Request. See Encoder for more information.
	Encode Encoder

	// Decode decodes messages from the Response into a byte slice. See
	// Decoder for more information.
	Decode Decoder

	// Cancel cancels the context of a client stream, the only way to abort it.
	// Close calls it to end the stream and unblock a pending Read. If it's nil,
	// Close only half-closes a client stream with CloseSend, so a pending Read
	// waits for the other side to end the stream.
	// Leave it nil for a server stream, which ends when its handler returns.
	// And the server handler should return once Done is closed.
	Cancel context.CancelFunc

	// Local and Remote network addresses of the connection. These are not available
	// from a gRPC stream so you must set them if you want to use them. They
	// LocalAddr and RemoteAddr return these values if set, otherwise they return
	// a default Addr.
	Local, Remote net.Addr

	// mu protects done and err.
	mu sync.Mutex
	// done is closed when the connection ends. It's created lazily so that
	// a Conn can be used without a constructor.
	done chan struct{}
	// err is the reason the connection ended. It's nil while it's open.
	err error

	// readOffset tracks where we've read up to if we're reading a result
	// that didn't fully fit into the target slice. See Read.
	readOffset int

	// locks to ensure that only one reader/writer are operating at a time.
	// Go documents the `net.Conn` interface as being safe for simultaneous
	// readers/writers so we need to implement locking.
	readLock, writeLock sync.Mutex
}

// Read implements io.Reader.
func (c *Conn) Read(p []byte) (int, error) {
	c.readLock.Lock()
	defer c.readLock.Unlock()

	if err := c.Err(); err != nil {
		return 0, err
	}

	// Attempt to read a value only if we're not still decoding a
	// partial read value from the last result.
	if c.readOffset == 0 {
		if err := c.Stream.RecvMsg(c.Response); err != nil {
			c.finish(err)
			return 0, err
		}
	}

	// Decode into our slice
	data, err := c.Decode(c.Response, c.readOffset, p)

	// If we have an error or we've decoded the full amount then we're done.
	// The error case is obvious. The case where we've read the full amount
	// is also a terminating condition and err == nil (we know this since its
	// checking that case) so we return the full amount and no error.
	if err != nil || len(data) <= (len(p)+c.readOffset) {
		// Reset the read offset since we're done.
		n := len(data) - c.readOffset
		c.readOffset = 0

		// Reset our response value for the next read and so that we
		// don't potentially store a large response structure in memory.
		proto.Reset(c.Response)

		// A message that can't be decoded leaves the stream in an unknown state.
		if err != nil {
			c.finish(err)
		}
		return n, err
	}

	// We didn't read the full amount so we need to store this for future reads
	c.readOffset += len(p)

	return len(p), nil
}

// Write implements io.Writer.
func (c *Conn) Write(p []byte) (int, error) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	if err := c.Err(); err != nil {
		return 0, err
	}

	total := len(p)
	for {
		// Encode our data into the request. Any error means we abort.
		n, err := c.Encode(c.Request, p)
		if err != nil {
			c.finish(err)
			return 0, err
		}

		// We lock for SendMsg if we have a lock set.
		if c.ResponseLock != nil {
			c.ResponseLock.Lock()
		}

		// Send our message. Any error we also just abort out.
		err = c.Stream.SendMsg(c.Request)
		if c.ResponseLock != nil {
			c.ResponseLock.Unlock()
		}
		if err != nil {
			// A client stream returns io.EOF when the stream has ended for
			// another reason. Unread messages and the actual status are still
			// available from RecvMsg, so let Read end the connection.
			if !errors.Is(err, io.EOF) {
				c.finish(err)
			}
			return 0, err
		}

		// If we sent the full amount of data, we're done. We respond with
		// "total" in case we sent across multiple frames.
		if n == len(p) {
			return total, nil
		}

		// We sent partial data so we continue writing the remainder
		p = p[n:]
	}
}

// Close ends the connection and closes Done. Reads and writes after that
// return net.ErrClosed.
//
// If Cancel is set, Close calls it to end the stream. Otherwise, it calls
// CloseSend on a client stream, so read the documentation for that to
// understand the semantics of this call. A server stream ends only when the
// RPC handler returns, so the handler should return once Done is closed.
func (c *Conn) Close() error {
	c.finish(net.ErrClosed)

	if c.Cancel != nil {
		c.Cancel()
		return nil
	}
	if cs, ok := c.Stream.(grpc.ClientStream); ok {
		// We have to acquire the write lock since the gRPC docs state:
		// "It is also not safe to call CloseSend concurrently with SendMsg."
		c.writeLock.Lock()
		defer c.writeLock.Unlock()
		return cs.CloseSend()
	}

	return nil
}

// Done returns a channel that is closed when the connection ends. See Conn
// for when that happens.
func (c *Conn) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done == nil {
		c.done = make(chan struct{})
	}
	return c.done
}

// Err returns the reason the connection ended or nil if it's still open.
// It's io.EOF if the other side ended the stream normally and net.ErrClosed
// if Close was called.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.err
}

// finish ends the connection with err unless it has already ended.
func (c *Conn) finish(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return
	}
	c.err = err

	if c.done == nil {
		c.done = make(chan struct{})
	}
	close(c.done)
}

// LocalAddr returns Local or an Addr if it's nil.
func (c *Conn) LocalAddr() net.Addr {
	if c.Local != nil {
		return c.Local
	}
	return Addr{}
}

// RemoteAddr returns Remote or an Addr if it's nil.
func (c *Conn) RemoteAddr() net.Addr {
	if c.Remote != nil {
		return c.Remote
	}
	return Addr{}
}

// SetDeadline is non-functional due to limitations on how gRPC works.
// You can mimic deadlines often using call options.
func (c *Conn) SetDeadline(time.Time) error { return nil }

// SetReadDeadline is non-functional, see SetDeadline.
func (c *Conn) SetReadDeadline(time.Time) error { return nil }

// SetWriteDeadline is non-functional, see SetDeadline.
func (c *Conn) SetWriteDeadline(time.Time) error { return nil }

var _ net.Conn = (*Conn)(nil)

// Addr is the default address of both ends of a Conn. A gRPC stream doesn't
// expose the addresses of the underlying connection.
type Addr struct{}

// Network returns "grpc-stream".
func (Addr) Network() string { return "grpc-stream" }

// String returns "grpc-stream".
func (Addr) String() string { return "grpc-stream" }
