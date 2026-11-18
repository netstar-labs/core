//go:build !linux && !darwin

package main

func availableMemory() int64 { return 0 }
