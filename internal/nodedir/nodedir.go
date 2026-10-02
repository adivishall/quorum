// Package nodedir is a node's data directory as a whole (audit H1): the lock
// that makes it one process's, and the identity that makes it one node's.
//
// Raft's safety rests on durable state: a node that forgot its vote can vote
// twice in a term, and one that forgot entries it acknowledged can let a
// committed entry be lost. So a node must never run on durable state that is
// not its own, and must never run on NO durable state while its peers believe
// it holds some. A data directory therefore:
//
//   - is locked (an exclusive flock on DIR/LOCK) for as long as a process
//     uses it, so two processes never open one log;
//   - names its node and its cluster in DIR/node.identity, written once when
//     the directory is initialized, and a process configured as another node
//     or for another cluster refuses it;
//   - is initialized only on request. A directory with no identity and no
//     state is a NEW node only when the operator says so (-init); otherwise
//     it is a node whose state was lost — wiped, replaced, or a wrong path —
//     and starting it empty, under its old id, would let it vote and
//     acknowledge as if it held the state its peers count on. It is refused,
//     and the node must be replaced through a membership change.
//
// Initialization is crash-safe: the identity is recorded first with
// Initialized false, the node creates its genesis state, and the identity is
// rewritten with Initialized true. A restart that finds an unfinished
// initialization resumes it — the identity file proves an operator initialized
// THIS directory; only a directory with no identity at all needs -init.
//
//	one record (kind 1): magic "QNOD" | version | flags | len | node id | len | cluster id
//
// Integers are canonical uvarints; flags bit 0 is Initialized.
package nodedir

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/vfs"
)

// File names inside a data directory.
const (
	IdentityFile = "node.identity"
	LockFile     = "LOCK"
	identityTmp  = "node.identity.tmp"
)

// Bounds of an identity.
const (
	MaxNodeIDLen    = 256
	MaxClusterIDLen = 128
)

const (
	identityMagic   = "QNOD"
	identityVersion = 1
	identityKind    = record.Kind(1)
	flagInitialized = 1
	maxIdentityFile = record.HeaderSize + len(identityMagic) + 3*binary.MaxVarintLen64 + MaxNodeIDLen + MaxClusterIDLen + 1
)

var (
	// ErrLocked: another process holds the data directory.
	ErrLocked = errors.New("nodedir: the data directory is in use by another process")
	// ErrIdentity: the identity file is corrupt, or names another node or
	// cluster than this process is configured as.
	ErrIdentity = errors.New("nodedir: node identity")
	// ErrUninitialized: the directory holds no node — a new node needs -init;
	// a node whose state was lost must be replaced, not restarted empty.
	ErrUninitialized = errors.New("nodedir: the data directory holds no node")
	// ErrAlreadyInitialized: -init on a directory that already holds a node.
	ErrAlreadyInitialized = errors.New("nodedir: the data directory is already initialized")
	// ErrNotEmpty: the directory holds files that are not a node's.
	ErrNotEmpty = errors.New("nodedir: the data directory holds files that are not a Quorum node's")
	// ErrClusterID: a cluster id is missing where one is required, or malformed.
	ErrClusterID = errors.New("nodedir: cluster id")
)

// Identity is what a data directory records about its node.
type Identity struct {
	Node    string
	Cluster string
	// Initialized is false between the start of an initialization and the
	// node's genesis state being durable.
	Initialized bool
}

// Encode returns the identity file's bytes. They are deterministic.
func Encode(id Identity) ([]byte, error) {
	if err := checkIDs(id.Node, id.Cluster); err != nil {
		return nil, err
	}
	p := []byte(identityMagic)
	p = binary.AppendUvarint(p, identityVersion)
	var flags uint64
	if id.Initialized {
		flags |= flagInitialized
	}
	p = binary.AppendUvarint(p, flags)
	p = binary.AppendUvarint(p, uint64(len(id.Node)))
	p = append(p, id.Node...)
	p = binary.AppendUvarint(p, uint64(len(id.Cluster)))
	p = append(p, id.Cluster...)
	return record.Encode(nil, identityKind, p)
}

// Decode parses an identity file strictly: one intact record, this version,
// known flags, valid ids and nothing after. Anything else is ErrIdentity.
func Decode(b []byte) (Identity, error) {
	bad := func(format string, args ...any) (Identity, error) {
		return Identity{}, fmt.Errorf("%w: %s", ErrIdentity, fmt.Sprintf(format, args...))
	}
	rd := record.NewReader(bytes.NewReader(b), "node identity", int64(len(b)))
	kind, p, err := rd.Next()
	if err != nil {
		return bad("the record: %v", err)
	}
	if kind != identityKind {
		return bad("record kind %d", kind)
	}
	if rd.NextOffset() != int64(len(b)) {
		return bad("%d bytes after the record", int64(len(b))-rd.NextOffset())
	}
	if !bytes.HasPrefix(p, []byte(identityMagic)) {
		return bad("bad magic")
	}
	p = p[len(identityMagic):]
	uint := func() (uint64, bool) {
		v, n := binary.Uvarint(p)
		if n <= 0 || n != len(binary.AppendUvarint(nil, v)) {
			return 0, false
		}
		p = p[n:]
		return v, true
	}
	str := func(max int) (string, bool) {
		n, ok := uint()
		if !ok || n > uint64(max) || n > uint64(len(p)) {
			return "", false
		}
		s := string(p[:n])
		p = p[n:]
		return s, true
	}
	if v, ok := uint(); !ok || v != identityVersion {
		return bad("version %d (this build reads %d)", v, identityVersion)
	}
	flags, ok := uint()
	if !ok || flags&^flagInitialized != 0 {
		return bad("flags")
	}
	node, ok := str(MaxNodeIDLen)
	if !ok {
		return bad("node id")
	}
	cluster, ok := str(MaxClusterIDLen)
	if !ok {
		return bad("cluster id")
	}
	if len(p) != 0 {
		return bad("%d trailing bytes", len(p))
	}
	if err := checkIDs(node, cluster); err != nil {
		return Identity{}, err
	}
	return Identity{Node: node, Cluster: cluster, Initialized: flags&flagInitialized != 0}, nil
}

func checkIDs(node, cluster string) error {
	if node == "" || len(node) > MaxNodeIDLen {
		return fmt.Errorf("%w: node id of %d bytes", ErrIdentity, len(node))
	}
	return CheckClusterID(cluster)
}

// CheckClusterID reports whether s is a valid cluster id: 1 to MaxClusterIDLen
// bytes of letters, digits, '.', '_' and '-'.
func CheckClusterID(s string) error {
	if s == "" || len(s) > MaxClusterIDLen {
		return fmt.Errorf("%w: %q must be 1 to %d bytes", ErrClusterID, s, MaxClusterIDLen)
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return fmt.Errorf("%w: %q may hold only letters, digits, '.', '_' and '-'", ErrClusterID, s)
		}
	}
	return nil
}

// Load reads dir's identity file. found is false when there is none.
func Load(dir string) (id Identity, found bool, err error) {
	f, err := os.Open(filepath.Join(dir, IdentityFile))
	if errors.Is(err, fs.ErrNotExist) {
		return Identity{}, false, nil
	}
	if err != nil {
		return Identity{}, false, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, int64(maxIdentityFile)+1))
	if err != nil {
		return Identity{}, false, err
	}
	if len(b) > maxIdentityFile {
		return Identity{}, false, fmt.Errorf("%w: the file is over %d bytes", ErrIdentity, maxIdentityFile)
	}
	id, err = Decode(b)
	return id, err == nil, err
}

// write records id durably: a temporary file, fsynced, renamed over the
// identity file, and the directory fsynced — so a crash leaves the old file or
// the new one, never a torn one.
func write(dir string, id Identity) error {
	b, err := Encode(id)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, identityTmp)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, IdentityFile)); err != nil {
		return err
	}
	return vfs.OS{}.SyncDir(dir)
}

// Options configure Open.
type Options struct {
	Node string
	// Cluster is the cluster id. Initializing a directory, or adopting one
	// from before node identities, needs it; otherwise it may be empty (the
	// recorded one is used) or must equal the recorded one.
	Cluster string
	// Init initializes a new, empty data directory (the operator's -init).
	Init bool
}

// State says how Open found the directory.
type State int

const (
	// Running: an initialized directory of this node.
	Running State = iota
	// Initializing: a new directory being initialized — by this start, or by
	// an earlier start that died before finishing. The node may create its
	// genesis state; FinishInit records that it did.
	Initializing
	// Adopted: a directory written before node identities (it holds Raft
	// state and no identity file); its identity is recorded now.
	Adopted
)

func (s State) String() string {
	switch s {
	case Running:
		return "running"
	case Initializing:
		return "initializing"
	case Adopted:
		return "adopted"
	}
	return fmt.Sprintf("state(%d)", int(s))
}

// Dir is an opened, locked data directory.
type Dir struct {
	Path  string
	ID    Identity
	State State
	lock  *lock
}

// Open locks dir and applies the identity rules above. With Init, a directory
// that does not exist is created (mode 0700); without it, one that does not
// exist is refused and nothing is created. The lock is held until Close.
func Open(dir string, opts Options) (*Dir, error) {
	if opts.Node == "" || len(opts.Node) > MaxNodeIDLen {
		return nil, fmt.Errorf("%w: node id of %d bytes", ErrIdentity, len(opts.Node))
	}
	if opts.Cluster != "" {
		if err := CheckClusterID(opts.Cluster); err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) && !opts.Init {
		// Nothing to lock or to read, and nothing is created: a mistyped
		// path must not leave a directory behind.
		return nil, fmt.Errorf("%w: %s does not exist; pass -init to initialize a new node there. "+
			"If node %q ran here before, its durable state is gone: replace it through a membership change", ErrUninitialized, dir, opts.Node)
	}
	if err := mkdirDurable(dir); err != nil {
		return nil, err
	}
	l, err := acquire(filepath.Join(dir, LockFile))
	if err != nil {
		return nil, err
	}
	d := &Dir{Path: dir, lock: l}
	if err := d.open(opts); err != nil {
		_ = l.release()
		return nil, err
	}
	return d, nil
}

func (d *Dir) open(opts Options) error {
	if err := os.Remove(filepath.Join(d.Path, identityTmp)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	id, found, err := Load(d.Path)
	if err != nil {
		return err
	}
	if found {
		switch {
		case id.Node != opts.Node:
			return fmt.Errorf("%w: %s belongs to node %q, this process is node %q", ErrIdentity, d.Path, id.Node, opts.Node)
		case opts.Cluster != "" && opts.Cluster != id.Cluster:
			return fmt.Errorf("%w: %s belongs to cluster %q, this process is configured for %q", ErrIdentity, d.Path, id.Cluster, opts.Cluster)
		case id.Initialized && opts.Init:
			return fmt.Errorf("%w: %s already holds node %q of cluster %q; -init initializes a new, empty data directory only", ErrAlreadyInitialized, d.Path, id.Node, id.Cluster)
		}
		d.ID, d.State = id, Running
		if !id.Initialized {
			d.State = Initializing
		}
		return nil
	}
	legacy, err := d.scan(opts.Node)
	if err != nil {
		return err
	}
	switch {
	case legacy && opts.Init:
		return fmt.Errorf("%w: %s holds Raft state; -init initializes a new, empty data directory only", ErrAlreadyInitialized, d.Path)
	case !legacy && !opts.Init:
		return fmt.Errorf("%w: %s holds no node; pass -init to initialize a new node there. "+
			"If node %q ran here before, its durable state is gone: it must not rejoin under that id as if it "+
			"held that state — remove it from its groups and add a new member instead", ErrUninitialized, d.Path, opts.Node)
	case opts.Cluster == "":
		return fmt.Errorf("%w: initializing or adopting a data directory needs a cluster id (-cluster-id)", ErrClusterID)
	}
	d.ID = Identity{Node: opts.Node, Cluster: opts.Cluster}
	if legacy {
		d.ID.Initialized, d.State = true, Adopted
	} else {
		d.State = Initializing
	}
	return write(d.Path, d.ID)
}

// scan classifies a directory with no identity file: legacy Raft state (a
// raft-<id>.log of this node, or a groups/ directory), or empty. Another
// node's log, or files that are not a node's, are refused.
func (d *Dir) scan(node string) (legacy bool, err error) {
	entries, err := os.ReadDir(d.Path)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case name == LockFile:
		case name == "groups" && e.IsDir():
			legacy = true
		case strings.HasPrefix(name, "raft-") && strings.Contains(name, ".log"):
			owner := strings.TrimPrefix(name[:strings.Index(name, ".log")], "raft-")
			if owner != node {
				return false, fmt.Errorf("%w: %s holds node %q's log (%s), this process is node %q", ErrIdentity, d.Path, owner, name, node)
			}
			legacy = true
		default:
			return false, fmt.Errorf("%w: %s holds %q", ErrNotEmpty, d.Path, name)
		}
	}
	return legacy, nil
}

// Initializing reports whether this start may create the node's genesis state.
func (d *Dir) Initializing() bool { return d.State == Initializing }

// FinishInit records that the node's genesis state is durable: the directory
// is initialized, and a later start neither needs nor accepts -init.
func (d *Dir) FinishInit() error {
	if d.State != Initializing {
		return nil
	}
	id := d.ID
	id.Initialized = true
	if err := write(d.Path, id); err != nil {
		return err
	}
	d.ID, d.State = id, Running
	return nil
}

// Close releases the lock.
func (d *Dir) Close() error { return d.lock.release() }

// mkdirDurable creates dir and any missing parents (mode 0700), fsyncing each
// new directory's parent so a power loss cannot make the directory vanish.
func mkdirDurable(dir string) error {
	var missing []string
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		missing = append(missing, d)
		if filepath.Dir(d) == d {
			break
		}
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
		if err := (vfs.OS{}).SyncDir(filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}
