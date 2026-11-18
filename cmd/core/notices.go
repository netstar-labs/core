package main

import (
	"embed"
	"fmt"
	"io"
)

// noticesCfg is the (flagless) config for the `notices` subcommand — cli.Command
// reflects on Cfg, so it may not be nil even when a command takes no flags.
var noticesCfg struct{}

// noticeFS embeds the third-party license notices so they travel inside the binary.
//
// This is the compliance path for a distributed binary. The source tree carries the
// notices in internal/*/LICENSE and THIRD-PARTY-NOTICES (BSD clause 1), but a binary
// built from it is separable from both. Clause 2 (identical in BSD-3-Clause and
// BSD-2-Clause) covers that case, and it is not optional:
//
//	Redistributions in binary form must reproduce the above copyright notice, this
//	list of conditions and the following disclaimer in the documentation and/or other
//	materials provided with the distribution.
//
// A recipient may get only the binary, scp'd on its own. A notices
// file sitting next to it in the build directory is separable from it and will get
// separated. Embedding makes the obligation travel with the artifact inseparably, and
// `core notices` is then always able to satisfy it. build/core.sh also writes the file
// into the delivery, so the "documentation and/or other materials" limb is covered
// whichever way the artifact is packaged.
//
//go:embed all:notices
var noticeFS embed.FS

// writeNotices prints every embedded notice.
func writeNotices(w io.Writer) error {
	entries, err := noticeFS.ReadDir("notices")
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "core — third-party notices\n")
	fmt.Fprintf(w, "%s\n\n", "==========================")
	fmt.Fprintf(w, "core itself is licensed under the Apache License, Version 2.0 (see LICENSE).\n")
	fmt.Fprintf(w, "The notices below cover third-party code compiled into this binary, whose\n")
	fmt.Fprintf(w, "licenses require them to accompany it.\n\n")
	for _, e := range entries {
		b, err := noticeFS.ReadFile("notices/" + e.Name())
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "--- %s ---\n\n", e.Name())
		if _, err := w.Write(b); err != nil {
			return err
		}
		fmt.Fprintln(w)
	}
	return nil
}
