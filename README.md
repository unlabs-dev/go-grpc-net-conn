# go-grpc-net-conn [![Godoc](https://godoc.org/github.com/unlabs-dev/go-grpc-net-conn?status.svg)](https://godoc.org/github.com/unlabs-dev/go-grpc-net-conn)

go-grpc-net-conn is a Go library that creates a `net.Conn` implementation
on top of gRPC streams. If the stream is bidirectional (both the request and
response of an RPC is a stream) then the `net.Conn` is a
[full-duplex connection](https://en.wikipedia.org/wiki/Duplex_(telecommunications)#Full_duplex).

## Installation

Standard `go get`:

```
$ go get github.com/unlabs-dev/go-grpc-net-conn
```

## Usage & Example

For usage and examples see the [Godoc](http://godoc.org/github.com/unlabs-dev/go-grpc-net-conn).

A brief example is shown below. Note that the only minor complexity is
populating the required fields for the `Conn` structure. This package needs
to know how to encode and decode the byte slices onto your expected protobuf
message types.

Imagine a protobuf service that looks like the following:

```proto
syntax = "proto3";

package example;

service ExampleService {
  rpc Stream(stream Bytes) returns (stream Bytes);
}

message Bytes {
  bytes data = 1;
}
```

You can use this in the following way:

```go
// On the client side, call our streaming endpoint with a context that conn.Close can cancel.
ctx, cancel := context.WithCancel(context.Background())
resp, err := client.Stream(ctx)

// We need to create a callback so the conn knows how to decode/encode
// arbitrary byte slices for our proto type.
fieldFunc := func(msg proto.Message) *[]byte {
	return &msg.(*example.Bytes).Data
}

// Wrap our conn around the response.
conn := &grpc_net_conn.Conn{
	Stream:   resp,
	Request:  &example.Bytes{},
	Response: &example.Bytes{},
	Encode:   grpc_net_conn.SimpleEncoder(fieldFunc),
	Decode:   grpc_net_conn.SimpleDecoder(fieldFunc),
	Cancel:   cancel,
}

// conn implements net.Conn so use it as you would!
```

On the server side, wrap the received stream in a `Conn` as well. A server stream only ends
when its RPC handler returns, so return once `conn.Done()` is closed:

```go
func (s *server) Stream(stream example.ExampleService_StreamServer) error {
	conn := &grpc_net_conn.Conn{
		Stream:   stream,
		Request:  &example.Bytes{},
		Response: &example.Bytes{},
		Encode:   grpc_net_conn.SimpleEncoder(fieldFunc),
		Decode:   grpc_net_conn.SimpleDecoder(fieldFunc),
	}
	// conn implements net.Conn so use it as you would!
	go handle(conn)

	<-conn.Done()
	return nil
}
```
