// main.go — Hosts the Dispatcher and Consolidator as gRPC servers.
// Usage: go run . <datafile> <M> <N> <C> <configfile>
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	pb "cmsc_621_project_2/primes"

	"google.golang.org/grpc"
)

// ─────────────────────────────────────────────
// CLI args
// ─────────────────────────────────────────────

func parseArgs() (datafile string, M, N, C int, configPath string) {
	flag.StringVar(&datafile,   "data",   "",               "path to binary datafile")
	flag.IntVar   (&M,          "M",      4,                "number of workers")
	flag.IntVar   (&N,          "N",      64*1024,          "segment size in bytes")
	flag.IntVar   (&C,          "C",      1024,             "chunk size in bytes")
	flag.StringVar(&configPath, "config", "primes_config.txt", "path to config file")
	flag.Parse()

	// Also support positional args like P1: <datafile> <M> <N> <C> <config>
	if flag.NArg() >= 5 {
		datafile   = flag.Arg(0)
		fmt.Sscan(flag.Arg(1), &M)
		fmt.Sscan(flag.Arg(2), &N)
		fmt.Sscan(flag.Arg(3), &C)
		configPath = flag.Arg(4)
	}
	return
}

// ─────────────────────────────────────────────
// Dispatcher server
// ─────────────────────────────────────────────

type dispatcherServer struct {
	pb.UnimplementedDispatcherServer
	jobQueue chan *pb.JobResponse // buffered; dispatcher goroutine fills it
}

func (s *dispatcherServer) PullJob(_ interface{}, req *pb.JobRequest) (*pb.JobResponse, error) {
	// TODO: replace the two lines below with the real channel receive
	// job, ok := <-s.jobQueue
	// if !ok { return &pb.JobResponse{HasJob: false}, nil }
	// return job, nil
	panic("TODO: implement PullJob — receive from s.jobQueue")
}

// fillJobs partitions the datafile into segments and pushes them onto jobQueue.
// This replaces your P1 dispatcher goroutine; run it as: go fillJobs(...)
func fillJobs(datafile string, N int, jobQueue chan<- *pb.JobResponse) {
	defer close(jobQueue)

	info, err := os.Stat(datafile)
	if err != nil {
		slog.Error("stat datafile", "err", err)
		return
	}
	fileSize := info.Size()

	// TODO: walk the file in N-byte segments, build JobResponse for each,
	//       and send on jobQueue. Same logic as your P1 dispatcher goroutine.
	_ = fileSize
	slog.Info("dispatcher: all jobs enqueued")
}

// ─────────────────────────────────────────────
// Consolidator server
// ─────────────────────────────────────────────

type consolidatorServer struct {
	pb.UnimplementedConsolidatorServer
	resultQueue chan *pb.ResultRequest
}

func (s *consolidatorServer) PushResult(_ interface{}, req *pb.ResultRequest) (*pb.ResultResponse, error) {
	// TODO: send req onto s.resultQueue; return accepted=true
	panic("TODO: implement PushResult — send to s.resultQueue")
}

// consolidate drains resultQueue and sums up prime counts.
// Signal done when all M workers have finished.
func consolidate(resultQueue <-chan *pb.ResultRequest, M int, done chan<- int64) {
	// TODO: accumulate prime counts until you've received M "worker done" signals
	//       (or track some other termination condition), then send total on done.
	panic("TODO: implement consolidate")
}

// ─────────────────────────────────────────────
// Server startup helpers
// ─────────────────────────────────────────────

func startGRPCServer(addr string, registerFn func(*grpc.Server)) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "addr", addr, "err", err)
		os.Exit(1)
	}
	srv := grpc.NewServer()
	registerFn(srv)
	slog.Info("gRPC server listening", "addr", addr)
	if err := srv.Serve(lis); err != nil {
		slog.Error("server error", "err", err)
	}
}

// ─────────────────────────────────────────────
// main
// ─────────────────────────────────────────────

func main() {
	datafile, M, N, C, configPath := parseArgs()
	slog.Info("starting", "datafile", datafile, "M", M, "N", N, "C", C, "config", configPath)

	cfg, err := ParseConfig(configPath)
	if err != nil {
		slog.Error("parse config", "err", err)
		os.Exit(1)
	}

	// Shared channels (same implementation as in project 1, just now behind gRPC handlers)
	jobQueue    := make(chan *pb.JobResponse, M*2)
	resultQueue := make(chan *pb.ResultRequest, M*2)
	done        := make(chan int64, 1)

	// 1. Fill jobs in background (your P1 dispatcher logic)
	go fillJobs(datafile, N, jobQueue)

	// 2. Start Dispatcher gRPC server
	dispSrv := &dispatcherServer{jobQueue: jobQueue}
	go startGRPCServer(cfg.Dispatcher, func(s *grpc.Server) {
		pb.RegisterDispatcherServer(s, dispSrv)
	})

	// 3. Start Consolidator gRPC server
	consSrv := &consolidatorServer{resultQueue: resultQueue}
	go startGRPCServer(cfg.Consolidator, func(s *grpc.Server) {
		pb.RegisterConsolidatorServer(s, consSrv)
	})

	// 4. Run consolidation logic
	go consolidate(resultQueue, M, done)

	// 5. Wait for servers to come up before workers connect
	time.Sleep(200 * time.Millisecond)

	// 6. Wait for the final answer
	start := time.Now()
	var wg sync.WaitGroup
	_ = wg   // wg not needed here — just waiting on done channel
	_ = C    // C is passed to workers via CLI, not used directly in main
	totalPrimes := <-done

	elapsed := time.Since(start).Milliseconds()
	fmt.Printf("Total primes: %d\nElapsed: %d ms\n", totalPrimes, elapsed)
}
