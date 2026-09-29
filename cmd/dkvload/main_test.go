package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adivishall/quorum/internal/kv"
)

type okServer struct{}

func (okServer) Name() string { return "ok" }
func (okServer) Do(_ context.Context, req kv.Request) (kv.Response, error) {
	if req.Op == kv.ReqRegister {
		return kv.Response{Status: kv.StatusOK, ClientID: 1, Index: 1}, nil
	}
	return kv.Response{Status: kv.StatusOK, Index: 1}, nil
}

// TestRunWritesAResult: against a protocol server, dkvload runs, prints its
// summary and writes a JSON result with the environment and the outcomes;
// bad flags exit 2.
func TestRunWritesAResult(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go kv.Serve(ctx, ln, okServer{}, nil)
	out := filepath.Join(t.TempDir(), "r.json")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-endpoints", "n1=" + ln.Addr().String(), "-clients", "2",
		"-duration", "300ms", "-warmup", "100ms", "-read", "50", "-keys", "50", "-out", out, "-repo", "../.."}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "completed") || !strings.Contains(stdout.String(), "get") {
		t.Fatalf("summary:\n%s", stdout.String())
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var rep struct {
		Env struct {
			GoVersion string `json:"go_version"`
		} `json:"env"`
		Result struct {
			Classes map[string]int64 `json:"classes"`
		} `json:"result"`
	}
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Env.GoVersion == "" || rep.Result.Classes["ok"] == 0 {
		t.Fatalf("result: %s", b)
	}
	for _, args := range [][]string{{}, {"-endpoints", "nonsense"}, {"-endpoints", "n1=x:1", "-read", "90", "-delete", "20"}} {
		if code := run(context.Background(), args, &stdout, &stderr); code != 2 {
			t.Fatalf("%v: exit %d, want 2", args, code)
		}
	}
}
