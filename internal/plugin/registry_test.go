package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type greeter interface{ Greet() string }

type hello struct{ msg string }

func (h hello) Greet() string { return h.msg }

type closing struct {
	hello
	log *[]string
}

func (c closing) Close() error { *c.log = append(*c.log, c.msg); return nil }

var testKind = NewKind[greeter]("test_greeter", true, "test")

func init() {
	Register(testKind, "hello", "says hello", func(_ context.Context, _ *Host, c Config) (greeter, error) {
		cfg := struct {
			Msg string `json:"msg"`
		}{Msg: "hello"}
		if err := c.Decode(&cfg); err != nil {
			return nil, err
		}
		return hello{cfg.Msg}, nil
	})
	Register(testKind, "fails", "always fails", func(context.Context, *Host, Config) (greeter, error) {
		return nil, errors.New("boom")
	})
}

func TestBuildDecodesConfigStrictly(t *testing.T) {
	h := NewHost("all", nil)
	g, err := Build(context.Background(), testKind, h, NewSpec("hello", map[string]string{"msg": "hi"}))
	if err != nil || g.Greet() != "hi" {
		t.Fatalf("got %v %v", g, err)
	}
	if _, err := Build(context.Background(), testKind, h, Spec{Name: "hello", Config: json.RawMessage(`{"typo":1}`)}); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field must fail, got %v", err)
	}
	if _, err := Build(context.Background(), testKind, h, Spec{Name: "nope"}); err == nil || !strings.Contains(err.Error(), "not compiled in") {
		t.Fatalf("unknown plugin: %v", err)
	}
	if _, err := Build(context.Background(), testKind, h, Spec{Name: "fails"}); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("factory error: %v", err)
	}
	if got := h.Active(); len(got) != 1 || got[0].Name != "hello" {
		t.Fatalf("active = %+v", got)
	}
}

func TestDuplicateRegistrationPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	Register(testKind, "hello", "dup", func(context.Context, *Host, Config) (greeter, error) { return hello{}, nil })
}

func TestSpecsForms(t *testing.T) {
	var s struct{ A, B, C, D Specs }
	err := json.Unmarshal([]byte(`{"A":"x","B":{"name":"y","config":{"k":1}},"C":["x",{"name":"y","enabled":false}],"D":null}`), &s)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.A) != 1 || s.A[0].Name != "x" || s.B[0].Name != "y" || string(s.B[0].Config) != `{"k":1}` {
		t.Fatalf("%+v", s)
	}
	if len(s.C) != 2 || len(s.C.Enabled()) != 1 || s.D != nil {
		t.Fatalf("%+v", s.C)
	}
	if err := json.Unmarshal([]byte(`{"nme":"x"}`), new(Specs)); err == nil {
		t.Fatal("typo in spec must fail")
	}
}

func TestBuildAllAndLifecycle(t *testing.T) {
	var closed []string
	k := NewKind[greeter]("test_closing", true, "")
	Register(k, "c", "", func(_ context.Context, _ *Host, c Config) (greeter, error) {
		var cfg struct{ Msg string }
		_ = c.Decode(&cfg)
		return closing{hello{cfg.Msg}, &closed}, nil
	})
	h := NewHost("all", nil)
	specs := Specs{{Name: "c", Instance: "one", Config: json.RawMessage(`{"Msg":"1"}`)}, {Name: "c", Instance: "two", Config: json.RawMessage(`{"Msg":"2"}`)}}
	gs, err := BuildAll(context.Background(), k, h, specs)
	if err != nil || len(gs) != 2 {
		t.Fatal(err)
	}
	if _, err := BuildAll(context.Background(), k, h, Specs{{Name: "c"}, {Name: "c"}}); err == nil {
		t.Fatal("duplicate instance must fail")
	}
	if _, err := BuildOne(context.Background(), k, h, specs); err == nil {
		t.Fatal("BuildOne with two specs must fail")
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(closed, ",") != "2,1" {
		t.Fatalf("closers must run in reverse order, got %v", closed)
	}
}

type job struct{ ran chan struct{} }

func (j job) Run(ctx context.Context) error { close(j.ran); return nil }

func TestRunnersAreCollected(t *testing.T) {
	k := NewKind[Runner]("test_job", true, "")
	ran := make(chan struct{})
	Register(k, "j", "", func(context.Context, *Host, Config) (Runner, error) { return job{ran}, nil })
	h := NewHost("all", nil)
	if _, err := Build(context.Background(), k, h, Spec{Name: "j"}); err != nil {
		t.Fatal(err)
	}
	rs := h.Runners()
	if len(rs) != 1 {
		t.Fatalf("runners = %v", rs)
	}
	for _, r := range rs {
		_ = r.Run(context.Background())
	}
	<-ran
}
