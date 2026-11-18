//go:build linux

package main

import (
	"os"
	"strconv"
	"strings"
)

func hostMemory() int64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 2 {
			return 0
		}
		if n, err := strconv.ParseInt(f[1], 10, 64); err == nil {
			return n * 1024
		}
	}
	return 0
}
