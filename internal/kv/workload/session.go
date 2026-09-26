package workload

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/adivishall/quorum/internal/kv"
	"github.com/adivishall/quorum/internal/lincheck"
)

// runSession is a client under the Phase 13 policy: register a session, then
// send each request with its identity through kv.Session, which retries an
// unknown outcome under the same identity. One logical request is one recorded
// operation; every send is one of its attempts.
func (c *client) runSession(ctx context.Context) Stats {
	doers := make([]kv.Doer, 0, len(c.eps))
	for _, ep := range c.eps {
		d, ok := ep.(kv.Doer)
		if !ok {
			panic(fmt.Sprintf("workload: session mode needs kv.Doer endpoints; %T is not one", ep))
		}
		doers = append(doers, d)
	}
	// Start each client's round-robin at a different node, as run does.
	rot := append(append([]kv.Doer(nil), doers[c.next%len(doers):]...), doers[:c.next%len(doers)]...)
	sess, err := kv.Register(ctx, rot, c.sessionOptions())
	if err != nil {
		return c.st // the cluster never became available to this client
	}
	c.st.Sessions++
	for i := 0; i < c.opts.OpsPerClient; i++ {
		if ctx.Err() != nil || (c.opts.Stop != nil && c.opts.Stop()) {
			break
		}
		kind, key, value := c.nextOp(i)
		rid := sess.Reserve()
		if kind != lincheck.Get && c.opts.DupPct > 0 && c.rng.Intn(100) < c.opts.DupPct {
			// A deliberate concurrent duplicate: the same request on another
			// connection, recorded as its own op with the same identity.
			sess.Hold(rid)
			c.st.DupSends++
			var wg sync.WaitGroup
			wg.Add(1)
			others := rot
			if len(rot) > 1 {
				others = rot[1:]
			}
			// The duplicate's session view: this client has nothing else in
			// flight (it issues one request at a time), so everything below
			// rid is acknowledged.
			dup := kv.ResumeSession(others, c.sessionOptions(), sess.ID(), rid+1)
			dup.Hold(rid)
			go func() {
				defer wg.Done()
				defer dup.Release(rid)
				st := c.sessionOp(ctx, dup, rid, kind, key, value, c.name+"-dup")
				c.merge(st)
			}()
			st := c.sessionOp(ctx, sess, rid, kind, key, value, c.name)
			wg.Wait()
			c.merge(st)
			sess.Release(rid)
		} else {
			c.merge(c.sessionOp(ctx, sess, rid, kind, key, value, c.name))
		}
		sess.Release(rid)
		if c.opts.Pace > 0 {
			select {
			case <-time.After(c.opts.Pace):
			case <-ctx.Done():
			}
		}
	}
	return c.st
}

func (c *client) sessionOptions() kv.SessionOptions {
	return kv.SessionOptions{AttemptTimeout: c.opts.Timeout, MaxAttempts: c.opts.MaxAttempts, Backoff: c.opts.Backoff}
}

var statsMu sync.Mutex

func (c *client) merge(st Stats) {
	statsMu.Lock()
	c.st.add(st)
	statsMu.Unlock()
}

// sessionOp runs one send stream of logical request rid and records it.
func (c *client) sessionOp(ctx context.Context, sess *kv.Session, rid uint64, kind lincheck.Kind, key string, value []byte, name string) Stats {
	var st Stats
	st.Ops++
	id := c.rec.BeginRequest(name, kind, key, value, sess.ID(), rid)
	op := map[lincheck.Kind]kv.ReqOp{lincheck.Put: kv.ReqPut, lincheck.Get: kv.ReqGet, lincheck.Delete: kv.ReqDelete}[kind]
	unknownSeen := false
	hook := func(node string) func(kv.Response, error) {
		a := c.rec.Attempt(id, node)
		st.Attempts++
		return func(resp kv.Response, err error) {
			switch {
			case err != nil && errors.Is(err, kv.ErrUnavailable):
				c.rec.AttemptDone(id, a, true, "unavailable", 0)
				st.Unavailable++
			case err != nil:
				c.rec.AttemptDone(id, a, false, "unknown: "+err.Error(), 0)
				st.Unknown++
				unknownSeen = true
			default:
				complete := resp.Status != kv.StatusUnknown
				result := strings.ToLower(resp.Status.String())
				if resp.Duplicate {
					result += " (duplicate of index " + fmt.Sprint(resp.Index) + ")"
					st.Duplicates++
				}
				if resp.Via != "" {
					result += " via " + resp.Via
					st.Forwarded++
				}
				if resp.Status == kv.StatusNotLeader {
					result += "(" + resp.Leader + ")"
					st.Redirects++
				}
				if resp.Status == kv.StatusUnknown {
					st.Unknown++
					unknownSeen = true
				}
				c.rec.AttemptDone(id, a, complete, result, resp.Term)
			}
			if unknownSeen && kind != lincheck.Get {
				st.WriteRetries++ // the next attempt, if any, is a retry of an unknown write
			}
		}
	}
	out := sess.Send(ctx, rid, op, []byte(key), value, hook)
	resp := out.Response
	switch {
	case out.Err == nil && resp.Status == kv.StatusOK:
		var output []byte
		if kind == lincheck.Get {
			output = resp.Value
		}
		c.rec.End(id, lincheck.OK, output, resp.Node, resp.Term, resp.Index)
		st.OK++
	case out.Err == nil:
		c.rec.End(id, lincheck.NotFound, nil, resp.Node, resp.Term, resp.Index)
		st.NotFound++
	case !out.Known:
		c.rec.End(id, lincheck.Incomplete, nil, "", 0, 0)
		st.Incomplete++
	default:
		c.rec.End(id, lincheck.Rejected, nil, resp.Node, resp.Term, resp.Index)
		st.Rejected++
		if errors.Is(out.Err, kv.ErrLost) {
			st.Lost++
		}
	}
	return st
}

// nextOp draws the next operation exactly as run does.
func (c *client) nextOp(i int) (lincheck.Kind, string, []byte) {
	key := fmt.Sprintf("k%d", c.rng.Intn(max(c.opts.Keys, 1)))
	var kind lincheck.Kind
	switch p := c.rng.Intn(100); {
	case p < c.opts.GetPct:
		kind = lincheck.Get
	case p < c.opts.GetPct+c.opts.DeletePct:
		kind = lincheck.Delete
	default:
		kind = lincheck.Put
	}
	var value []byte
	if kind == lincheck.Put {
		value = []byte(fmt.Sprintf("%s-%d", c.name, i))
	}
	return kind, key, value
}
