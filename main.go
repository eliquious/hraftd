package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/eliquious/hraftd/pb"
	"github.com/eliquious/hraftd/store"
)

// Command line defaults
const (
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
)

// Command line parameters
var httpAddr string
var raftAddr string
var joinAddr string

func init() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	runtime.GOMAXPROCS(8)
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	// Ensure Raft storage exists.
	raftDir := flag.Arg(0)
	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	s := store.New()
	s.RaftDir = raftDir
	s.RaftBind = raftAddr
	if err := s.Open(joinAddr == ""); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	lis, err := net.Listen("tcp", httpAddr)
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}

	handler := &Handler{s}
	grpcServer := grpc.NewServer()
	pb.RegisterKeyValueServer(grpcServer, handler)
	go grpcServer.Serve(lis)

	// h := httpd.New(httpAddr, s)
	// if err := h.Start(); err != nil {
	// 	log.Fatalf("failed to start HTTP service: %s", err.Error())
	// }

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(joinAddr, raftAddr); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	}

	log.Println("hraft started successfully")

	// Block forever.
	select {}
}

func join(joinAddr, raftAddr string) error {
	conn, err := grpc.Dial(joinAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewKeyValueClient(conn)
	_, err = client.Join(context.Background(), &pb.JoinRequest{raftAddr})
	return err
}

type Handler struct {
	store *store.Store
}

func (h *Handler) Get(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	val, err := h.store.Get(r.Key)
	return &pb.GetResponse{Value: val}, err
}

func (h *Handler) Set(ctx context.Context, r *pb.SetRequest) (*pb.SetResponse, error) {
	err := h.store.Set(r.Key, r.Value)
	return &pb.SetResponse{}, err
}

func (h *Handler) Delete(ctx context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return &pb.DeleteResponse{}, h.store.Delete(r.Key)
}

func (h *Handler) Join(ctx context.Context, r *pb.JoinRequest) (*pb.JoinResponse, error) {
	return &pb.JoinResponse{}, h.store.Join(r.RemoteAddr)
}

func (h *Handler) Stream(stream pb.KeyValue_StreamServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if err = h.store.Do(in); err != nil {
			return err
		}

		if err := stream.Send(&pb.StreamResponse{}); err != nil {
			return err
		}
	}
}
