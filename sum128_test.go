package core

import (
	"encoding/hex"
	"testing"
)

func TestSum128(t *testing.T) {
	// XXH3-128, canonical big-endian, from the reference libxxhash 0.8.2.
	for in, want := range map[string]string{
		"example.com": "c481d7301ccf29bb4bfbb7c48c9c3712",
		"hello":       "b5e9c1ad071b3e7fc779cfaa5e523818",
		"":            "99aa06d3014798d86001c324468d497f",
	} {
		a, b := Sum128([]byte(in)), Sum128String(in)
		if a != b {
			t.Errorf("Sum128 != Sum128String for %q", in)
		}
		if got := hex.EncodeToString(a[:]); got != want {
			t.Errorf("Sum128(%q) = %s; want %s", in, got, want)
		}
	}
	if n := testing.AllocsPerRun(100, func() { _ = Sum128String("example.com") }); n != 0 {
		t.Errorf("Sum128String allocates %v per call", n)
	}
}
