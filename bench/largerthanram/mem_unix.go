//go:build linux || darwin

package main

import "syscall"

// osResident reports the process resident set size from the OS, so a capped run can
// be shown to have actually been capped rather than merely flagged.
func osResident() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	// Maxrss is KiB on linux and bytes on darwin — normalise.
	if isDarwin {
		return int64(ru.Maxrss)
	}
	return int64(ru.Maxrss) * 1024
}
