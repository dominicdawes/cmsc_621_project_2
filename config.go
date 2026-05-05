// config.go — put this file in EVERY binary's package (copy it in)
// or move it to a shared internal package if you prefer.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Config holds the address (host:port) of each service.
type Config struct {
	Dispatcher  string // e.g. "localhost:5001"
	Consolidator string // e.g. "localhost:5002"
	FileServer  string // e.g. "localhost:5003"
}

// ParseConfig reads a file in the format:
//   dispatcher   host0 5001
//   consolidator host0 5002
//   fileserver   host1 5003
func ParseConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	cfg := &Config{}
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 3 {
			return nil, fmt.Errorf("bad config line: %q", line)
		}
		addr := parts[1] + ":" + parts[2]
		switch strings.ToLower(parts[0]) {
		case "dispatcher":
			cfg.Dispatcher = addr
		case "consolidator":
			cfg.Consolidator = addr
		case "fileserver":
			cfg.FileServer = addr
		default:
			return nil, fmt.Errorf("unknown service %q in config", parts[0])
		}
	}
	return cfg, scanner.Err()
}