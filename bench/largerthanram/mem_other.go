//go:build !linux && !darwin

package main

// osResident is unavailable here; the caller falls back to the Go heap view.
func osResident() int64 { return 0 }
