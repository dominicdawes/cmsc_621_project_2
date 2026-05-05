// Dominic Dawes — CMSC 621 Project 2

// fileserver/main.go — Standalone FileServer process.
// Usage: go run . <configfile>
package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"

	pb "cmsc_621_project_2/primes"

	"google.golang.org/grpc"
)

// ─────────────────────────────────────────────
// FileServer implementation
// ─────────────────────────────────────────────

type fileServer struct {
	pb.UnimplementedFileServerServer
}

// FetchSegment streams chunks of the requested file segment back to the caller.
// This is a server-side streaming RPC — note the stream parameter.
func (s *fileServer) FetchSegment(req *pb.ChunkRequest, stream pb.FileServer_FetchSegmentServer) error {
	slog.Info("FetchSegment", "file", req.Filepath, "start", req.Start, "len", req.Length, "chunk", req.ChunkSize)

	f, err := os.Open(req.Filepath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	// Seek to the start of the segment
	if _, err := f.Seek(req.Start, io.SeekStart); err != nil {
		return fmt.Errorf("seek: %w", err)
	}

	buf       := make([]byte, req.ChunkSize)
	remaining := req.Length

	for remaining > 0 {
		toRead := req.ChunkSize
		if remaining < toRead {
			toRead = remaining
		}

		n, err := f.Read(buf[:toRead])
		if n > 0 {
			// Send this chunk down the stream
			if sendErr := stream.Send(&pb.Chunk{Data: buf[:n]}); sendErr != nil {
				return sendErr
			}
			remaining -= int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
	}

	return nil // stream closes automatically when we return nil
}

// ─────────────────────────────────────────────
// main
// ─────────────────────────────────────────────

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: fileserver <configfile>")
		os.Exit(1)
	}
	configPath := os.Args[1]

	cfg, err := ParseConfig(configPath)
	if err != nil {
		slog.Error("parse config", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", cfg.FileServer)
	if err != nil {
		slog.Error("listen failed", "addr", cfg.FileServer, "err", err)
		os.Exit(1)
	}

	srv := grpc.NewServer()
	pb.RegisterFileServerServer(srv, &fileServer{})

	slog.Info("FileServer listening", "addr", cfg.FileServer)
	if err := srv.Serve(lis); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}