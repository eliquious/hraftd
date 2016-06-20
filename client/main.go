package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eliquious/hraftd/pb"
	"google.golang.org/grpc"

	"github.com/alecthomas/kingpin"
	"golang.org/x/net/context"
)

var (
	app      = kingpin.New("chat", "A command-line chat application.")
	serverIP = kingpin.Flag("server", "Leader address.").Required().String()
	stream   = kingpin.Flag("stream", "Stream requests").Bool()
)

func main() {
	runtime.GOMAXPROCS(8)
	kingpin.Parse()
	var writes, written uint64
	ctx, _ := context.WithTimeout(context.Background(), time.Second*240)
	ticker := time.Tick(time.Second)

	var wg sync.WaitGroup
	for cIndex := 0; cIndex < 8; cIndex++ {
		conn, err := grpc.Dial(*serverIP, grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		client := pb.NewKeyValueClient(conn)

		for index := 0; index < 32; index++ {
			wg.Add(1)
			if *stream {

				// client := p.Get()
				go func(c context.Context, client pb.KeyValueClient) {
					defer wg.Done()
					stream, err := client.Stream(ctx)
					if err != nil {
						fmt.Println(err)
						return
					}

					for {
						select {
						case <-c.Done():
							return
						default:
							if err := stream.Send(&pb.Command{Op: pb.CommandOp_SET, Key: "key", Value: "value"}); err != nil {
								return
							}
							if _, err := stream.Recv(); err != nil {
								return
							}
						}
						atomic.AddUint64(&writes, 1)
					}
				}(ctx, client)
			} else {
				go func(c context.Context, client pb.KeyValueClient) {
					defer wg.Done()
					// client := p.Get()

					for {
						select {
						case <-c.Done():
							return
						default:
							_, err := client.Set(ctx, &pb.SetRequest{Key: "key", Value: "value"})
							if err != nil {
								return
							}
						}
						atomic.AddUint64(&writes, 1)
					}
				}(ctx, client)
			}
		}
	}

OUTER:
	for {
		select {
		case _ = <-ticker:
			fmt.Println("Writes per Second: ", writes-written)
			written = writes
		case <-ctx.Done():
			break OUTER
		}
	}
	wg.Wait()
	fmt.Println("Writes: ", writes)
}

func NewFixedPool(size int) *FixedPool {
	var clients []pb.KeyValueClient
	for i := 0; i < size; i++ {
		conn, err := grpc.Dial(*serverIP, grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		client := pb.NewKeyValueClient(conn)
		clients = append(clients, client)
	}
	return &FixedPool{uint64(size - 1), 0, clients}
}

type FixedPool struct {
	size  uint64
	index uint64
	pool  []pb.KeyValueClient
}

func (f *FixedPool) Get() pb.KeyValueClient {
	index := atomic.AddUint64(&f.index, 1)
	return f.pool[index&f.size]
}

// Pool holds Clients.
type Pool struct {
	pool     chan *Client
	borrowed uint64
	returned uint64
}

// NewPool creates a new pool of Clients.
func NewPool(max int) *Pool {
	return &Pool{
		pool: make(chan *Client, max),
	}
}

// Borrow a Client from the pool.
func (p *Pool) Borrow() *Client {
	var c *Client
	select {
	case c = <-p.pool:
	default:
		conn, err := grpc.Dial(*serverIP, grpc.WithInsecure())
		if err != nil {
			panic(err)
		}

		client := pb.NewKeyValueClient(conn)
		c = &Client{conn, client}
	}
	return c
}

// Return returns a Client to the pool.
func (p *Pool) Return(c *Client) {
	select {
	case p.pool <- c:
	default:
		// let it go, let it go...
		c.conn.Close()
	}
}

type Client struct {
	conn   *grpc.ClientConn
	client pb.KeyValueClient
}
