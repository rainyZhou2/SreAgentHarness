package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
)

// Browser preview mode (SRE_DESKTOP_PREVIEW=127.0.0.1:34115): the same App
// and the same UI, served to a browser instead of a native window. A shim
// stands in for the Wails bridge: window.go.main.App.X(...) becomes
// POST /__call/X and runtime events arrive over /__events (SSE). Meant for
// UI development and automated checks; it binds to loopback only.

const previewShim = `"use strict";
(function () {
  const call = (name) => async (...args) => {
    const r = await fetch("/__call/" + name, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(args) });
    const out = await r.json();
    if (out.error) throw new Error(out.error);
    return out.result;
  };
  window.go = { main: { App: new Proxy({}, { get: (_, name) => call(name) }) } };
  const handlers = {};
  const es = new EventSource("/__events");
  es.onmessage = (m) => { const { name, data } = JSON.parse(m.data); (handlers[name] || []).forEach((f) => f(data)); };
  window.runtime = { EventsOn: (name, f) => { (handlers[name] = handlers[name] || []).push(f); } };
})();
`

func servePreview(addr string, app *App) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
		return fmt.Errorf("preview address must be loopback (got %q)", addr)
	}
	var mu sync.Mutex
	subs := map[chan []byte]bool{}
	app.emit = func(name string, data any) {
		b, _ := json.Marshal(map[string]any{"name": name, "data": data})
		mu.Lock()
		defer mu.Unlock()
		for ch := range subs {
			select {
			case ch <- b:
			default:
			}
		}
	}
	app.startup(context.Background())

	dist, _ := fs.Sub(assets, "frontend/dist")
	files := http.FileServer(http.FS(dist))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		b, err := fs.ReadFile(dist, "index.html")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		html := strings.Replace(string(b), `<script src="app.js">`, `<script src="/__preview.js"></script><script src="app.js">`, 1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(html))
	})
	mux.HandleFunc("GET /__preview.js", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/javascript")
		w.Write([]byte(previewShim))
	})
	mux.HandleFunc("GET /__events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		fl.Flush()
		ch := make(chan []byte, 256)
		mu.Lock()
		subs[ch] = true
		mu.Unlock()
		defer func() { mu.Lock(); delete(subs, ch); mu.Unlock() }()
		for {
			select {
			case <-r.Context().Done():
				return
			case b := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", b)
				fl.Flush()
			}
		}
	})
	mux.HandleFunc("POST /__call/{method}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		res, err := callMethod(app, r.PathValue("method"), r)
		out := map[string]any{"result": res}
		if err != nil {
			out = map[string]any{"error": err.Error()}
		}
		json.NewEncoder(w).Encode(out)
	})
	mux.Handle("GET /", files)
	log.Printf("browser preview on http://%s/", addr)
	return http.ListenAndServe(addr, mux)
}

// callMethod invokes an exported App method with JSON-decoded arguments,
// mirroring how Wails binds methods (last return value may be an error).
func callMethod(app *App, name string, r *http.Request) (any, error) {
	m := reflect.ValueOf(app).MethodByName(name)
	if !m.IsValid() || name == "" || strings.ToUpper(name[:1]) != name[:1] {
		return nil, fmt.Errorf("unknown method %q", name)
	}
	var raw []json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return nil, err
	}
	t := m.Type()
	if len(raw) != t.NumIn() {
		return nil, fmt.Errorf("%s: want %d args, got %d", name, t.NumIn(), len(raw))
	}
	args := make([]reflect.Value, len(raw))
	for i, a := range raw {
		v := reflect.New(t.In(i))
		if err := json.Unmarshal(a, v.Interface()); err != nil {
			return nil, fmt.Errorf("%s arg %d: %w", name, i, err)
		}
		args[i] = v.Elem()
	}
	outs := m.Call(args)
	var res any
	for _, o := range outs {
		if o.Type() == reflect.TypeFor[error]() {
			if !o.IsNil() {
				return nil, o.Interface().(error)
			}
			continue
		}
		res = o.Interface()
	}
	return res, nil
}
