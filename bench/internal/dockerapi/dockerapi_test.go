package dockerapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseIOStat(t *testing.T) {
	text := "259:0 rbytes=100 wbytes=2000 rios=3 wios=4 dbytes=0 dios=0\n8:16 rbytes=1 wbytes=500 rios=1 wios=1 dbytes=0 dios=0\n"
	got, err := ParseIOStat(text)
	if err != nil || got != 2500 {
		t.Fatalf("ParseIOStat = %d, %v; want 2500", got, err)
	}
	if _, err := ParseIOStat(""); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("an empty io.stat is %v, want ErrUnavailable", err)
	}
	if _, err := ParseIOStat("1:1 wbytes=abc"); err == nil {
		t.Fatal("a number that is not one was accepted")
	}
}

func TestCgroupWriteBytesFromAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "io.stat")
	if _, err := CgroupWriteBytes(path); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a missing file is %v, want ErrUnavailable", err)
	}
	if err := os.WriteFile(path, []byte("1:2 rbytes=0 wbytes=4096 rios=0 wios=1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if n, err := CgroupWriteBytes(path); err != nil || n != 4096 {
		t.Fatalf("CgroupWriteBytes = %d, %v", n, err)
	}
}

func TestParseStatsWrites(t *testing.T) {
	body := []byte(`{"blkio_stats":{"io_service_bytes_recursive":[
		{"major":8,"minor":0,"op":"read","value":10},
		{"major":8,"minor":0,"op":"write","value":300},
		{"major":8,"minor":16,"op":"Write","value":20}]}}`)
	if n, err := ParseStatsWrites(body); err != nil || n != 320 {
		t.Fatalf("ParseStatsWrites = %d, %v; want 320", n, err)
	}
	if _, err := ParseStatsWrites([]byte(`{"blkio_stats":{"io_service_bytes_recursive":null}}`)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("stats without block figures are %v, want ErrUnavailable", err)
	}
	if _, err := ParseStatsWrites([]byte(`not json`)); err == nil {
		t.Fatal("bad JSON was accepted")
	}
}

// fakeDaemon serves the little of the API that the client uses on a unix socket.
func fakeDaemon(t *testing.T) (socket string, restarted chan string) {
	t.Helper()
	// Unix socket paths are short on macOS, so the directory is a short one.
	dir, err := os.MkdirTemp("", "kvb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket = filepath.Join(dir, "d.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Skipf("unix sockets are not available: %v", err)
	}
	restarted = make(chan string, 4)
	mux := http.NewServeMux()
	mux.HandleFunc("/_ping", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("OK")) })
	mux.HandleFunc("/containers/db/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("stream") != "false" {
			http.Error(w, "must not stream", http.StatusBadRequest)
			return
		}
		w.Write([]byte(`{"blkio_stats":{"io_service_bytes_recursive":[{"op":"write","value":777}]}}`))
	})
	mux.HandleFunc("/containers/db/restart", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "post", http.StatusMethodNotAllowed)
			return
		}
		restarted <- r.URL.Query().Get("t")
		w.WriteHeader(http.StatusNoContent)
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return socket, restarted
}

func TestClientAgainstAFakeDaemon(t *testing.T) {
	socket, restarted := fakeDaemon(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := New(socket)
	if !c.Available(ctx) {
		t.Fatal("the daemon does not answer")
	}
	if n, err := c.WriteBytes(ctx, "db"); err != nil || n != 777 {
		t.Fatalf("WriteBytes = %d, %v", n, err)
	}
	if _, err := c.WriteBytes(ctx, "nope"); err == nil {
		t.Fatal("a container that is not there had stats")
	}
	if err := c.Restart(ctx, "db", 7*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := <-restarted; got != "7" {
		t.Fatalf("the timeout of the restart was %q, want 7", got)
	}

	m := ContainerMeter(ctx, c, "db")
	if !m.Available() || m.Source != "docker-api" {
		t.Fatalf("meter = %+v", m)
	}
	if n, err := m.Read(ctx); err != nil || n != 777 {
		t.Fatalf("meter read = %d, %v", n, err)
	}
	if m := ContainerMeter(ctx, c, "nope"); m.Available() || m.Source != "unavailable" {
		t.Fatalf("a container with no stats has a meter: %+v", m)
	}
}

func TestNoDaemonIsUnavailable(t *testing.T) {
	ctx := context.Background()
	c := New(filepath.Join(t.TempDir(), "none.sock"))
	if c.Available(ctx) {
		t.Fatal("a daemon answers on a socket that is not there")
	}
	m := ContainerMeter(ctx, c, "db")
	if m.Available() || m.Source != "unavailable" {
		t.Fatalf("meter = %+v", m)
	}
	if _, err := m.Read(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("read = %v, want ErrUnavailable", err)
	}
}
