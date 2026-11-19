package grpc_net_conn

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/unlabs-dev/go-grpc-net-conn/testproto"
)

func TestConn(t *testing.T) {
	impl := &testServer{
		Send: [][]byte{
			[]byte("hello"),
			[]byte("bye"),
		},
	}

	t.Run("full data", func(t *testing.T) {
		conn := testStreamConn(testStreamClient(t, impl))
		data := make([]byte, 1024)
		n, err := conn.Read(data)
		require.NoError(t, err)
		require.Equal(t, len("hello"), n)
		require.Equal(t, "hello", string(data[:n]))
	})

	t.Run("partial read", func(t *testing.T) {
		conn := testStreamConn(testStreamClient(t, impl))
		data := make([]byte, 3)

		// Read first time partial
		n, err := conn.Read(data)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, "hel", string(data[:n]))

		// Read again full result
		n, err = conn.Read(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, "lo", string(data[:n]))

		// Read again next message
		n, err = conn.Read(data)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, "bye", string(data[:n]))
	})
}

func TestConn_chunkedWrites(t *testing.T) {
	impl := &testServer{
		Chunk: 3,
		Send: [][]byte{
			[]byte("hello"),
			[]byte("bye"),
		},
	}

	conn := testStreamConn(testStreamClient(t, impl))
	data := make([]byte, 1024)

	// We expect two chunks
	n, err := conn.Read(data)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "hel", string(data[:n]))

	n, err = conn.Read(data)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, "lo", string(data[:n]))

	n, err = conn.Read(data)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "bye", string(data[:n]))
}

type testServer struct {
	Send  [][]byte
	Chunk int
}

func (s *testServer) Stream(stream testproto.TestService_StreamServer) error {
	// Get our conn
	conn := testStreamConn(stream)
	if s.Chunk > 0 {
		conn.Encode = ChunkedEncoder(conn.Encode, s.Chunk)
	}

	for _, data := range s.Send {
		if _, err := conn.Write(data); err != nil {
			return err
		}
	}

	<-stream.Context().Done()
	return nil
}

var _ testproto.TestServiceServer = (*testServer)(nil)

func TestConn_endedByServer(t *testing.T) {
	conns := make(chan *Conn, 1)
	impl := &closingServer{conns: conns}
	conn := testStreamConn(testStreamClient(t, impl))

	// The client sends first so that the server handler starts.
	_, err := conn.Write([]byte("hello"))
	require.NoError(t, err)
	serverConn := <-conns

	// Closing a server conn closes Done so the handler returns and ends the stream.
	require.NoError(t, serverConn.Close())
	require.ErrorIs(t, serverConn.Err(), net.ErrClosed)

	_, err = conn.Read(make([]byte, 1024))
	require.ErrorIs(t, err, io.EOF)
	select {
	case <-conn.Done():
	default:
		t.Fatal("Done isn't closed after the stream ended")
	}
	require.ErrorIs(t, conn.Err(), io.EOF)

	// Reads and writes after the end return the same error.
	_, err = conn.Read(make([]byte, 1024))
	require.ErrorIs(t, err, io.EOF)
	_, err = conn.Write([]byte("bye"))
	require.ErrorIs(t, err, io.EOF)
}

func TestConn_closeCancelsClientStream(t *testing.T) {
	grpcConn, server := testGRPCConn(t, func(s *grpc.Server) {
		testproto.RegisterTestServiceServer(s, &testServer{})
	})
	t.Cleanup(func() { server.Stop() })
	t.Cleanup(func() { grpcConn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := testproto.NewTestServiceClient(grpcConn).Stream(ctx)
	require.NoError(t, err)
	conn := testStreamConn(stream)
	conn.Cancel = cancel
	require.NoError(t, conn.Err())

	// The server never sends anything so the read blocks until Close cancels the stream.
	readErr := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1024))
		readErr <- err
	}()

	require.NoError(t, conn.Close())

	select {
	case err := <-readErr:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close didn't unblock a pending read")
	}
	<-conn.Done()
	require.ErrorIs(t, conn.Err(), net.ErrClosed)
	_, err = conn.Write([]byte("hello"))
	require.ErrorIs(t, err, net.ErrClosed)
}

func TestConn_addrs(t *testing.T) {
	conn := &Conn{}
	require.Equal(t, Addr{}, conn.LocalAddr())
	require.Equal(t, Addr{}, conn.RemoteAddr())
	require.Equal(t, "grpc-stream", conn.RemoteAddr().String())

	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
	conn = &Conn{Local: local, Remote: remote}
	require.Equal(t, local, conn.LocalAddr())
	require.Equal(t, remote, conn.RemoteAddr())
}

// closingServer hands the server conn to the test and returns when the conn is closed.
type closingServer struct {
	conns chan *Conn
}

func (s *closingServer) Stream(stream testproto.TestService_StreamServer) error {
	conn := testStreamConn(stream)
	s.conns <- conn
	<-conn.Done()
	return nil
}
