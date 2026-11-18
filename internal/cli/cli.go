// Package cli binds command-line flags to a tagged config struct using only the
// standard library (flag, reflect, os).
//
// A config struct is a plain struct whose exported fields are the flags:
//
//	type cfg struct {
//		Data  string `env:"order,require" help:"input path"`
//		Store string `default:"kv.core" help:"output store prefix"`
//		Cap   int    `env:"c" default:"1024" help:"initial capacity"`
//		Text  bool   `help:"input is text"`
//	}
//
// The flag name is the lowercased field name. Tags:
//
//	env:"alias,order,require"   any of: a short alias; order = also fillable from a
//	                            positional argument; require = must end up set
//	                            (env:"-" skips the field)
//	default:"value"             value used when nothing else sets the field
//	help:"text"                 description shown by the help table
//
// Accepted forms: -flag value, -flag=value, -flag:value, and a bare -flag for
// bools. Bools also accept on/yes/ok/true/1 as true and anything else as false,
// so -text:on and -flush:off work. An uppercased environment variable named after
// the flag (SHARDS=16) overrides the flag; an unparsable one is warned about and
// ignored. Positional arguments fill the order fields that no flag set, in
// declaration order, and any remainder is returned to the caller.
//
// Supported field types: string, bool, int, int64, uint, uint64, float32, float64.
package cli

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

// Version and Build are stamped at link time:
//
//	go build -ldflags "-X github.com/netstar-labs/core/internal/cli.Version=v1 \
//	                   -X github.com/netstar-labs/core/internal/cli.Build=abc123"
var (
	Version = "dev"
	Build   = "unknown"
)

// field is one bound config field.
type field struct {
	name, alias string
	help, def   string
	hasDef      bool
	order, req  bool
	v           reflect.Value
	set         bool // set explicitly by a flag or env var (defaults do not count)
}

// Set implements flag.Value for a field and records that it was set explicitly.
func (f *field) Set(s string) error {
	if err := assign(f.v, s); err != nil {
		return err
	}
	f.set = true
	return nil
}

func (f *field) String() string {
	if f == nil || !f.v.IsValid() {
		return ""
	}
	return fmt.Sprint(f.v.Interface())
}

// IsBoolFlag lets a bare -flag mean true and keeps it from swallowing the next token.
func (f *field) IsBoolFlag() bool { return f.v.Kind() == reflect.Bool }

// assign parses s into v. Bools are lenient: on/yes/ok/true/1 enable, anything
// else disables.
func assign(v reflect.Value, s string) error {
	switch v.Kind() {
	case reflect.String:
		v.SetString(s)
	case reflect.Bool:
		switch strings.ToLower(s) {
		case "on", "yes", "ok", "true", "1":
			v.SetBool(true)
		default:
			v.SetBool(false)
		}
	case reflect.Int, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer %q", s)
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid unsigned integer %q", s)
		}
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("invalid number %q", s)
		}
		v.SetFloat(n)
	default:
		return fmt.Errorf("unsupported field type %s", v.Type())
	}
	return nil
}

// fields reflects over cfg (a pointer to a struct) and applies the default tags.
func fields(cfg any) []*field {
	rv := reflect.ValueOf(cfg)
	if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Struct {
		panic("cli: config must be a pointer to a struct")
	}
	rv = rv.Elem()
	var out []*field
	for i := 0; i < rv.NumField(); i++ {
		sf := rv.Type().Field(i)
		if !sf.IsExported() {
			continue
		}
		f := &field{name: strings.ToLower(sf.Name), v: rv.Field(i), help: sf.Tag.Get("help")}
		if tag, ok := sf.Tag.Lookup("env"); ok {
			if tag == "-" {
				continue
			}
			for _, t := range strings.Split(tag, ",") {
				switch t {
				case "":
				case "order":
					f.order = true
				case "require":
					f.req = true
				default:
					f.alias = t
				}
			}
		}
		if d, ok := sf.Tag.Lookup("default"); ok {
			f.def, f.hasDef = d, true
			if err := assign(f.v, d); err != nil {
				panic(fmt.Sprintf("cli: bad default for %s: %v", f.name, err))
			}
		}
		out = append(out, f)
	}
	return out
}

// normalize rewrites -flag:value to -flag=value so the standard flag package can
// read the colon form. "=" wins when both appear, as in -codex=Age:4.
func normalize(args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		if strings.HasPrefix(a, "-") && !strings.Contains(a, "=") {
			a = strings.Replace(a, ":", "=", 1)
		}
		out[i] = a
	}
	return out
}

// Parse binds args (without the program or subcommand name) into cfg and returns the
// positional arguments left over after the order fields are filled.
func Parse(cfg any, args []string) (rest []string, err error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fl := fields(cfg)
	for _, f := range fl {
		fs.Var(f, f.name, f.help)
		if f.alias != "" {
			fs.Var(f, f.alias, f.help)
		}
	}

	// Everything after a literal "--" is positional; flags may otherwise be
	// interleaved with positionals ("lookup -store x key1 -codex y key2").
	var tail []string
	for i, a := range args {
		if a == "--" {
			args, tail = args[:i], args[i+1:]
			break
		}
	}
	args = normalize(args)
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) > 0 {
			rest = append(rest, args[0])
			args = args[1:]
		}
	}
	rest = append(rest, tail...)

	// Environment overrides the flags (documented behavior).
	for _, f := range fl {
		s, ok := os.LookupEnv(strings.ToUpper(f.name))
		if !ok || s == "" {
			continue
		}
		if err := f.Set(s); err != nil {
			fmt.Fprintf(os.Stderr, "%s: ignoring %s=%q: %v\n", prog(), strings.ToUpper(f.name), s, err)
		}
	}

	// Positionals fill the order fields that nothing else set.
	for _, f := range fl {
		if f.order && !f.set && len(rest) > 0 {
			if err := f.Set(rest[0]); err != nil {
				return nil, fmt.Errorf("%s: %w", f.name, err)
			}
			rest = rest[1:]
		}
	}

	for _, f := range fl {
		missing := !f.set && !f.hasDef
		if f.v.Kind() == reflect.String && f.v.String() == "" {
			missing = true
		}
		if f.req && missing {
			return nil, fmt.Errorf("missing required (%s) parameter", f.name)
		}
	}
	return rest, nil
}

// Usage writes the flag table for cfg: name, alias, [type], [order/require
// markers], (default) and help text.
func Usage(w io.Writer, cfg any) {
	for _, f := range fields(cfg) {
		var o, r string
		if f.order {
			o = "o"
		}
		if f.req {
			r = "r"
		}
		def := ""
		if f.hasDef && f.def != "" {
			def = "(" + f.def + ")"
		}
		fmt.Fprintf(w, " %-15s %-5s [%s] [%-1s%-1s  ] %-10s %s\n",
			f.name, f.alias, typeCode(f.v), o, r, def, f.help)
	}
}

func typeCode(v reflect.Value) string {
	switch v.Kind() {
	case reflect.String:
		return "s"
	case reflect.Int, reflect.Int64:
		return "i"
	case reflect.Uint, reflect.Uint64:
		return "u"
	case reflect.Float32, reflect.Float64:
		return "f"
	case reflect.Bool:
		return "b"
	}
	return " "
}

func prog() string { return filepath.Base(os.Args[0]) }

// PrintVersion writes the build stamp line.
func PrintVersion(w io.Writer) {
	fmt.Fprintf(w, "%s version %s build %s\n", prog(), Version, Build)
}

// IsHelp reports whether s is a help word: help, h, -h, -help, --help.
func IsHelp(s string) bool {
	switch strings.TrimLeft(s, "-") {
	case "help", "h":
		return true
	}
	return false
}

// IsVersion reports whether s is version, -version or --version.
func IsVersion(s string) bool { return strings.TrimLeft(s, "-") == "version" }

// Configure is the entry point for a single-purpose program: it answers version
// and help, parses os.Args[1:] into cfg, and exits with a message on a bad flag.
func Configure(cfg any) {
	if len(os.Args) > 1 {
		switch a := os.Args[1]; {
		case IsVersion(a):
			PrintVersion(os.Stdout)
			os.Exit(0)
		case IsHelp(a):
			fmt.Printf("usage: %s [flags]\n\n", prog())
			Usage(os.Stdout, cfg)
			fmt.Println()
			os.Exit(0)
		}
	}
	rest, err := Parse(cfg, os.Args[1:])
	if err == nil && len(rest) > 0 {
		err = fmt.Errorf("unexpected argument %q", rest[0])
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prog(), err)
		os.Exit(2)
	}
}

// Command is one subcommand: its name, one-line help, config struct, and handler.
type Command struct {
	Name string
	Help string
	Cfg  any                 // pointer to a tagged config struct
	Args bool                // accepts positional arguments beyond the order fields
	Run  func(rest []string) // called after Cfg is populated
}

// Dispatch runs the subcommand named by args[0]. The caller has already handled the
// bare invocation, the unknown-command menu and top-level help; Dispatch handles
// "version", "help <cmd>", "<cmd> help", and "<cmd> [flags]".
func Dispatch(cmds []Command, args []string) {
	find := func(name string) *Command {
		for i := range cmds {
			if cmds[i].Name == name {
				return &cmds[i]
			}
		}
		return nil
	}
	help := func(c *Command) {
		fmt.Printf("usage: %s %s [flags]\n\n", prog(), c.Name)
		if c.Help != "" {
			fmt.Printf("%s\n\n", c.Help)
		}
		Usage(os.Stdout, c.Cfg)
		fmt.Println()
	}

	switch {
	case IsVersion(args[0]):
		PrintVersion(os.Stdout)
		return
	case IsHelp(args[0]) && len(args) > 1 && find(args[1]) != nil:
		help(find(args[1]))
		return
	}
	c := find(args[0])
	if c == nil {
		fmt.Fprintf(os.Stderr, "%s: unknown command %q\n", prog(), args[0])
		os.Exit(2)
	}
	if len(args) > 1 && IsHelp(args[1]) {
		help(c)
		return
	}
	rest, err := Parse(c.Cfg, args[1:])
	if err == nil && len(rest) > 0 && !c.Args {
		err = fmt.Errorf("unexpected argument %q", rest[0])
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s %s: %v\n", prog(), c.Name, err)
		os.Exit(2)
	}
	c.Run(rest)
}
