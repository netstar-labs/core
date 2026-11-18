//go:build darwin

package main

// ru_maxrss is in bytes on darwin, unlike linux where it is KiB.
const isDarwin = true
