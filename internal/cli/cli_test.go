package cli

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

type tcfg struct {
	Data  string  `env:"order,require" help:"input"`
	Store string  `default:"kv.core" help:"store"`
	Cap   int     `env:"c" default:"1024"`
	Big   uint64  `default:"7"`
	Ratio float64 `default:"0.5"`
	Text  bool
	Flush bool   `default:"true"`
	Skip  string `env:"-"`
	priv  string
}

func parse(t *testing.T, args ...string) (tcfg, []string, error) {
	t.Helper()
	var c tcfg
	rest, err := Parse(&c, args)
	return c, rest, err
}

func TestForms(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want tcfg
		rest []string
	}{
		{"defaults+order", []string{"d"}, tcfg{Data: "d", Store: "kv.core", Cap: 1024, Big: 7, Ratio: 0.5, Flush: true}, nil},
		{"space", []string{"-data", "d", "-cap", "5"}, tcfg{Data: "d", Store: "kv.core", Cap: 5, Big: 7, Ratio: 0.5, Flush: true}, nil},
		{"equals", []string{"-data=d", "--store=s"}, tcfg{Data: "d", Store: "s", Cap: 1024, Big: 7, Ratio: 0.5, Flush: true}, nil},
		{"colon", []string{"-data:d", "-cap:9", "-ratio:1.5"}, tcfg{Data: "d", Store: "kv.core", Cap: 9, Big: 7, Ratio: 1.5, Flush: true}, nil},
		{"colon keeps later colons", []string{"-data:a:b:c"}, tcfg{Data: "a:b:c", Store: "kv.core", Cap: 1024, Big: 7, Ratio: 0.5, Flush: true}, nil},
		{"equals wins over colon", []string{"-data=a:b"}, tcfg{Data: "a:b", Store: "kv.core", Cap: 1024, Big: 7, Ratio: 0.5, Flush: true}, nil},
		{"alias", []string{"d", "-c", "3"}, tcfg{Data: "d", Store: "kv.core", Cap: 3, Big: 7, Ratio: 0.5, Flush: true}, nil},
		{"bare bool leaves next token positional", []string{"-text", "d"}, tcfg{Data: "d", Store: "kv.core", Cap: 1024, Big: 7, Ratio: 0.5, Flush: true, Text: true}, nil},
		{"bool on/off", []string{"d", "-text:on", "-flush:off"}, tcfg{Data: "d", Store: "kv.core", Cap: 1024, Big: 7, Ratio: 0.5, Text: true}, nil},
		{"bool yes/garbage", []string{"d", "-text=yes", "-flush=nope"}, tcfg{Data: "d", Store: "kv.core", Cap: 1024, Big: 7, Ratio: 0.5, Text: true}, nil},
		{"negative number value", []string{"d", "-cap", "-5"}, tcfg{Data: "d", Store: "kv.core", Cap: -5, Big: 7, Ratio: 0.5, Flush: true}, nil},
		{"flag wins order slot", []string{"-data", "x", "extra"}, tcfg{Data: "x", Store: "kv.core", Cap: 1024, Big: 7, Ratio: 0.5, Flush: true}, []string{"extra"}},
		{"interleaved positionals", []string{"d", "k1", "-store", "s", "k2"}, tcfg{Data: "d", Store: "s", Cap: 1024, Big: 7, Ratio: 0.5, Flush: true}, []string{"k1", "k2"}},
		{"double dash", []string{"d", "--", "-k", "-store"}, tcfg{Data: "d", Store: "kv.core", Cap: 1024, Big: 7, Ratio: 0.5, Flush: true}, []string{"-k", "-store"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, rest, err := parse(t, tc.args...)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("cfg\n got %+v\nwant %+v", got, tc.want)
			}
			if len(rest)+len(tc.rest) > 0 && !reflect.DeepEqual(rest, tc.rest) {
				t.Errorf("rest got %v want %v", rest, tc.rest)
			}
		})
	}
}

func TestEnvOverridesFlag(t *testing.T) {
	t.Setenv("STORE", "fromenv")
	t.Setenv("CAP", "") // empty is ignored
	got, _, err := parse(t, "d", "-store", "fromflag", "-cap", "4")
	if err != nil {
		t.Fatal(err)
	}
	if got.Store != "fromenv" || got.Cap != 4 {
		t.Errorf("got store=%q cap=%d", got.Store, got.Cap)
	}
}

func TestEnvInvalidIsIgnored(t *testing.T) {
	t.Setenv("CAP", "banana")
	got, _, err := parse(t, "d")
	if err != nil || got.Cap != 1024 {
		t.Errorf("cap=%d err=%v", got.Cap, err)
	}
}

func TestEnvSatisfiesRequire(t *testing.T) {
	t.Setenv("DATA", "e")
	if got, _, err := parse(t); err != nil || got.Data != "e" {
		t.Errorf("data=%q err=%v", got.Data, err)
	}
}

func TestErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"missing required", nil, "missing required (data)"},
		{"empty required", []string{"-data="}, "missing required (data)"},
		{"unknown flag", []string{"d", "-nope", "1"}, "nope"},
		{"bad int", []string{"d", "-cap", "x"}, "invalid"},
		{"bad uint", []string{"d", "-big", "-1"}, "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parse(t, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err=%v want contains %q", err, tc.want)
			}
		})
	}
}

func TestUsage(t *testing.T) {
	var b bytes.Buffer
	Usage(&b, &tcfg{})
	out := b.String()
	for _, want := range []string{
		" data                  [s] [or  ]            input\n",
		" store                 [s] [    ] (kv.core)  store\n",
		" cap             c     [i] [    ] (1024)     \n",
		" text                  [b] [    ]            \n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("usage missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "skip") || strings.Contains(out, "priv") {
		t.Errorf("usage lists a skipped field:\n%s", out)
	}
}
