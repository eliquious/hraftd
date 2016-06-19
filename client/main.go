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
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	ticker := time.Tick(time.Second)

	conn, err := grpc.Dial(*serverIP, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pb.NewKeyValueClient(conn)
	// res, err := client.Set(ctx, &pb.SetRequest{"user3", "max"})
	// fmt.Printf("%+v\n", res)
	// fmt.Printf("%+v\n", err)
	//
	// resGet, err := client.Get(ctx, &pb.GetRequest{"user3"})
	// fmt.Printf("%+v\n", resGet)
	// fmt.Printf("%+v\n", err)
	// return

	// pool := NewPool(512)
	// pool := NewFixedPool(8)
	var pool *FixedPool

	var wg sync.WaitGroup
	// pool := NewFixedPool(32)
	for index := 0; index < 1024; index++ {
		wg.Add(1)
		if *stream {

			go func(c context.Context, p *FixedPool) {
				defer wg.Done()
				// client := p.Get()
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
							// fmt.Println("Send error: ", err)
							return
						}
						if _, err := stream.Recv(); err != nil {
							// fmt.Println("Recv error: ", err)
							return
						}
						// _, err := client.Set(ctx, &pb.SetRequest{Key: "key", Value: "value"})
						// if err != nil {
						// 	// fmt.Println("Set error: ", err)
						// 	return
						// }
						// p.Return(client)
					}
					atomic.AddUint64(&writes, 1)
				}
			}(ctx, pool)
		} else {
			go func(c context.Context, p *FixedPool) {
				defer wg.Done()
				// client := p.Get()

				for {
					select {
					case <-c.Done():
						return
					default:
						_, err := client.Set(ctx, &pb.SetRequest{Key: "key", Value: "value"})
						if err != nil {
							// fmt.Println("Set error: ", err)
							return
						}
						// p.Return(client)
					}
					atomic.AddUint64(&writes, 1)
				}
			}(ctx, pool)
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
			// default:
			// client.Set(ctx, &pb.SetRequest{Key: "key", Value: "value"})
			// http.Post("http://localhost:11001/key", "application/json", bytes.NewReader(body))
			// writes++
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
