package patch

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/netstar-labs/core"
)

// Log is a durable, coalescing change accumulator for a long-running process
// (e.g. a server) streaming changes between flushes: a write-ahead log of
// fixed-size frames plus an in-RAM last-writer-wins index over its un-applied
// tail. Append changes as they arrive; Flush folds the coalesced set into a store
// in place and drops the consumed WAL segments.
//
// Durability is group-commit: an append lands in the in-process buffer, and a
// change is durable once a subsequent Sync (or the Sync inside Flush) returns. On
// restart, Open replays every un-applied segment, so a window that was accepted
// but not yet flushed survives a crash. Because Apply is idempotent over a
// coalesced snapshot, a crash mid-flush is recovered by replaying and re-applying
// the same window — the segment is removed only after the store Sync, so it is
// still present to replay if the process dies first.
//
// A WAL frame is op(1) | key(keyLen) | value(valLen); delete frames zero the
// value. Appends and Flush are safe to call concurrently; Flush is internally
// serialized.
type Log struct {
	dir            string
	keyLen, valLen int
	frame          int
	zero           []byte // valLen zero bytes, written as a delete frame's value

	mu      sync.Mutex
	cur     map[string]entry // coalesced view of all un-applied frames (prior + active)
	prior   []string         // sealed segments reflected in cur, pending removal at next Flush
	active  string           // segment currently receiving appends
	w       *os.File
	bw      *bufio.Writer
	idx     int
	pending int // bytes appended since the last Flush

	flushMu sync.Mutex // serializes Flush
}

const segPrefix = "wal."

func segName(idx int) string { return fmt.Sprintf("%s%06d", segPrefix, idx) }

func segIdx(path string) (int, bool) {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, segPrefix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(base, segPrefix))
	return n, err == nil
}

// Open opens (creating if needed) a Log in dir for a store of the given geometry,
// replaying any un-applied WAL segments left by a prior run into the coalescing
// index. The first Flush after Open consumes those replayed segments.
func Open(dir string, keyLen, valLen int) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &Log{
		dir: dir, keyLen: keyLen, valLen: valLen, frame: 1 + keyLen + valLen,
		zero: make([]byte, valLen), cur: make(map[string]entry),
	}

	matches, err := filepath.Glob(filepath.Join(dir, segPrefix+"*"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	maxIdx := -1
	for _, p := range matches {
		n, ok := segIdx(p)
		if !ok {
			continue
		}
		if err := l.replay(p); err != nil {
			return nil, err
		}
		l.prior = append(l.prior, p)
		if n > maxIdx {
			maxIdx = n
		}
	}
	l.idx = maxIdx + 1
	if err := l.openActive(l.idx); err != nil {
		return nil, err
	}
	return l, nil
}

// openActive opens segment idx for append and makes it the active segment.
func (l *Log) openActive(idx int) error {
	path := filepath.Join(l.dir, segName(idx))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.active, l.w, l.bw = path, f, bufio.NewWriterSize(f, 1<<16)
	return nil
}

// replay folds every whole frame in a segment into the coalescing index. A
// trailing partial frame (a torn write at a crash) is ignored.
func (l *Log) replay(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for i := 0; i+l.frame <= len(b); i += l.frame {
		f := b[i : i+l.frame]
		key := f[1 : 1+l.keyLen]
		if Op(f[0]) == OpDelete {
			l.cur[string(key)] = entry{op: OpDelete}
		} else {
			l.cur[string(key)] = entry{op: OpUpsert, val: append([]byte(nil), f[1+l.keyLen:]...)}
		}
	}
	return nil
}

// Upsert records key→val, appending a frame and coalescing in memory.
func (l *Log) Upsert(key, val []byte) error {
	if len(key) != l.keyLen || len(val) != l.valLen {
		return core.ErrBadSize
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.frameOut(OpUpsert, key, val); err != nil {
		return err
	}
	l.cur[string(key)] = entry{op: OpUpsert, val: append([]byte(nil), val...)}
	return nil
}

// Delete records the removal of key.
func (l *Log) Delete(key []byte) error {
	if len(key) != l.keyLen {
		return core.ErrBadSize
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.frameOut(OpDelete, key, nil); err != nil {
		return err
	}
	l.cur[string(key)] = entry{op: OpDelete}
	return nil
}

// frameOut writes one frame to the active segment buffer. Caller holds l.mu.
func (l *Log) frameOut(op Op, key, val []byte) error {
	if _, err := l.bw.Write([]byte{byte(op)}); err != nil {
		return err
	}
	if _, err := l.bw.Write(key); err != nil {
		return err
	}
	body := l.zero
	if op == OpUpsert {
		body = val
	}
	if _, err := l.bw.Write(body); err != nil {
		return err
	}
	l.pending += l.frame
	return nil
}

// Pending reports the number of distinct coalesced keys and the bytes appended
// since the last Flush — for size-triggered flush decisions.
//
// It is a queue-depth signal, NOT an applied-ness signal. Flush zeroes it when it
// rotates the segment, which happens *before* the coalesced window is applied to
// the store, so observing Pending() == 0 does not mean the changes are visible to
// a reader yet. A caller waiting for a flush to take effect must watch the store
// (a Get, Len, or Digest), not this counter.
//
// Nothing is lost in that gap: the sealed segment stays on disk until after the
// apply is durable, and Open replays an un-applied window. The gap is a visibility
// window, not a durability one.
func (l *Log) Pending() (changes, bytes int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.cur), l.pending
}

// Sync flushes the in-process buffer and fsyncs the active segment, making every
// append so far durable (group-commit point).
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.bw.Flush(); err != nil {
		return err
	}
	return l.w.Sync()
}

// Flush folds the accumulated changes into dst in place and drops the consumed
// WAL segments. It seals the active segment, swaps in a fresh one (so appends
// continue uninterrupted), makes the sealed segment durable, applies its
// coalesced snapshot to dst, Syncs dst, and only then removes the sealed
// segments. A crash before the removal leaves the segments to be replayed and
// re-applied (idempotently) on the next Open. Returns the apply Stats.
func (l *Log) Flush(dst *core.Sharded) (Stats, error) {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	// Pre-create the next active segment so a failure here leaves state intact.
	newIdx := l.idx + 1
	nf, err := os.OpenFile(filepath.Join(l.dir, segName(newIdx)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return Stats{}, err
	}

	l.mu.Lock()
	if err := l.bw.Flush(); err != nil {
		l.mu.Unlock()
		nf.Close()
		return Stats{}, err
	}
	oldW, oldActive, oldCur, oldPrior := l.w, l.active, l.cur, l.prior
	l.w, l.bw = nf, bufio.NewWriterSize(nf, 1<<16)
	l.active = filepath.Join(l.dir, segName(newIdx))
	l.idx = newIdx
	l.cur = make(map[string]entry)
	l.prior = nil
	l.pending = 0
	l.mu.Unlock()

	// Make the sealed segment durable, then apply its coalesced content.
	if err := oldW.Sync(); err != nil {
		oldW.Close()
		return Stats{}, err
	}
	oldW.Close()

	snap := snapshotFromMap(l.keyLen, l.valLen, oldCur, "")
	st, err := Apply(dst, snap)
	if err != nil {
		return st, err
	}
	if err := dst.Sync(); err != nil {
		return st, err
	}

	// Applied and durable: drop the consumed segments.
	os.Remove(oldActive)
	for _, p := range oldPrior {
		os.Remove(p)
	}
	return st, nil
}

// Close flushes and fsyncs the active segment, leaving any un-applied segments on
// disk for the next Open to replay. Call Flush before Close to apply pending
// changes; Close alone only persists the WAL.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bw != nil {
		if err := l.bw.Flush(); err != nil {
			return err
		}
	}
	if l.w != nil {
		if err := l.w.Sync(); err != nil {
			return err
		}
		return l.w.Close()
	}
	return nil
}
