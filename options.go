package core

// Option configures a table at Open time.
type Option func(*config)

type config struct {
	noSync   bool
	readOnly bool
}

func newConfig(opts []Option) config {
	var c config
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithNoSync disables the msync that otherwise makes a resize durable before the
// file is swapped in. It trades crash-durability for speed — ideal for bulk
// loading a rebuildable table, where a crash just means redo the load. Explicit
// Sync() still works and is honored regardless of this option.
func WithNoSync() Option { return func(c *config) { c.noSync = true } }

func (c config) doSync() bool { return !c.noSync }

// WithReadOnly opens an existing table for reading only: the file is opened
// O_RDONLY and mapped PROT_READ, no file is created, and every mutator returns
// [ErrReadOnly] (or false/0 where it has no error to return).
//
// It exists so a process that only serves lookups need not hold write access to
// the table. Without it every open is O_RDWR, so a read-only front end must be
// able to write the master it reads — and therefore cannot drop to an
// unprivileged user that lacks that permission.
//
// It does not make a table safe to read while another process writes it: that is
// the difference between [Open] and [OpenShared], and read-only changes neither.
func WithReadOnly() Option { return func(c *config) { c.readOnly = true } }
