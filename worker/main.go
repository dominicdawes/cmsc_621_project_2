
// Dominic Dawes — CMSC 621 Project 2

// worker/main.go — Standalone Worker process.
// Usage: go run . <C> <configfile>
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"time"

	pb "cmsc_621_project_2/primes"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

// dialGRPC opens a plain (no TLS) gRPC connection.
func dialGRPC(addr string) *grpc.ClientConn {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("dial failed", "addr", addr, "err", err)
		os.Exit(1)
	}
	return conn
}

// countPrimesInBytes decodes little-endian uint64s from raw bytes and counts primes.
// Reuse your P1 logic here exactly.
func countPrimesInBytes(data []byte) int64 {
	var count int64
	for i := 0; i+8 <= len(data); i += 8 {
		n := binary.LittleEndian.Uint64(data[i : i+8])
		if big.NewInt(0).SetUint64(n).ProbablyPrime(20) {
			count++
		}
	}
	return count
}

// ─────────────────────────────────────────────
// main
// ─────────────────────────────────────────────

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: worker <C> <configfile>")
		os.Exit(1)
	}

	var C int
	fmt.Sscan(os.Args[1], &C)
	configPath := os.Args[2]

	cfg, err := ParseConfig(configPath)
	if err != nil {
		slog.Error("parse config", "err", err)
		os.Exit(1)
	}

	workerID := fmt.Sprintf("worker-%d", os.Getpid())
	slog.Info("worker starting", "id", workerID, "C", C)

	// Connect to all three servers
	dispConn := dialGRPC(cfg.Dispatcher)
	defer dispConn.Close()

	fsConn := dialGRPC(cfg.FileServer)
	defer fsConn.Close()

	consConn := dialGRPC(cfg.Consolidator)
	defer consConn.Close()

	dispClient := pb.NewDispatcherClient(dispConn)
	fsClient   := pb.NewFileServerClient(fsConn)
	consClient := pb.NewConsolidatorClient(consConn)

	// P1-style: sleep 400-600ms before first job pull
	// TODO: add random sleep here (same as P1)
	time.Sleep(500 * time.Millisecond)

	// ── Main work loop ──────────────────────────────────────────────
	for {
		ctx := context.Background()

		// 1. Pull a job from the dispatcher
		job, err := dispClient.PullJob(ctx, &pb.JobRequest{WorkerId: workerID})
		if err != nil {
			slog.Error("PullJob failed", "err", err)
			break
		}
		if !job.HasJob {
			// Dispatcher signalled "no more jobs"
			slog.Info("no more jobs, shutting down", "id", workerID)
			break
		}
		slog.Info("got job", "id", workerID, "job_id", job.JobId, "start", job.Start, "len", job.Length)

		// 2. Fetch the segment in chunks via server-side streaming
		stream, err := fsClient.FetchSegment(ctx, &pb.ChunkRequest{
			Filepath:  job.Filepath,
			Start:     job.Start,
			Length:    job.Length,
			ChunkSize: int64(C),
		})
		if err != nil {
			slog.Error("FetchSegment failed", "err", err)
			continue
		}

		var primes int64
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				slog.Error("stream.Recv failed", "err", err)
				break
			}
			// 3. Count primes in this chunk as it arrives
			primes += countPrimesInBytes(chunk.Data)
		}

		slog.Info("job complete", "id", workerID, "job_id", job.JobId, "primes", primes)

		// 4. Push result to consolidator
		_, err = consClient.PushResult(ctx, &pb.ResultRequest{
			WorkerId:   workerID,
			JobId:      job.JobId,
			PrimeCount: primes,
		})
		if err != nil {
			slog.Error("PushResult failed", "err", err)
		}
	}

	slog.Info("worker done", "id", workerID)
}