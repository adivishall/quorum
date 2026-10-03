package raftnode

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/adivishall/quorum/internal/record"
	"github.com/adivishall/quorum/internal/replication"
	"github.com/adivishall/quorum/internal/vfs"
)

// Phase 15: the group identity file (docs/MEMBERSHIP.md §2, docs/MULTI_RAFT.md
// §5). Beside a node's Raft log, LogPath+".group" records, once and for all,
// which group the log belongs to and the group's genesis configuration — the
// configuration below the log's first entry. It is written durably at the
// node's first start, before the log exists, and never again: a restart reads
// it rather than trusting its flags, so a node restarted with a different
// member list, as a bootstrap member after it joined, or on another group's
// data cannot silently change its group's history. The configuration a node
// runs with is the latest configuration entry in its log, else its snapshot's,
// else this genesis (Raft §6).
//
//	one record (kind 1): magic "QGRP" | version | group | [len | node id] | len | genesis configuration
//
// Integers are canonical uvarints; the configuration is
// replication.EncodeConfiguration's bytes. A joiner's genesis is the empty
// configuration: it knows nothing of its group until the leader that adds it
// replicates to it.
//
// Version 2 (audit H1) records the node the state belongs to, and a node
// configured as another refuses it: a group's term, vote and log are one
// node's, and the genesis alone cannot tell — it is the same on every member.
// Version 1, with no node id, is still read (a group created before version 2)
// and is not rewritten; every new identity is version 2.

// ErrIdentity wraps a group identity file that is corrupt, absent where the
// node's durable state requires one, or contradicted by the node's
// configuration. The node does not start.
var ErrIdentity = errors.New("raftnode: group identity")

const (
	identityMagic     = "QGRP"
	identityVersionV1 = 1 // no node id
	identityVersion   = 2
	identityKind      = record.Kind(1)
	maxIdentityFile   = 4 + 4*binary.MaxVarintLen64 + replication.MaxMemberLen + replication.MaxEncodedConfiguration + record.HeaderSize
)

// Identity is a group's durable identity on one node.
type Identity struct {
	Group   replication.GroupID
	Genesis replication.Configuration
	// Node is the node whose state this is; empty only in a version-1 file.
	Node NodeID
}

func identityPath(logPath string) string    { return logPath + ".group" }
func identityTmpPath(logPath string) string { return logPath + ".group.tmp" }

// EncodeIdentity returns the identity file's bytes. They are deterministic.
func EncodeIdentity(id Identity) ([]byte, error) {
	if err := id.Genesis.Validate(); err != nil {
		return nil, fmt.Errorf("%w: genesis: %v", ErrIdentity, err)
	}
	if len(id.Node) > replication.MaxMemberLen {
		return nil, fmt.Errorf("%w: node id of %d bytes", ErrIdentity, len(id.Node))
	}
	p := []byte(identityMagic)
	if id.Node == "" {
		p = binary.AppendUvarint(p, identityVersionV1)
		p = binary.AppendUvarint(p, uint64(id.Group))
	} else {
		p = binary.AppendUvarint(p, identityVersion)
		p = binary.AppendUvarint(p, uint64(id.Group))
		p = binary.AppendUvarint(p, uint64(len(id.Node)))
		p = append(p, id.Node...)
	}
	conf := replication.EncodeConfiguration(id.Genesis)
	p = binary.AppendUvarint(p, uint64(len(conf)))
	p = append(p, conf...)
	return record.Encode(nil, identityKind, p)
}

// DecodeIdentity parses an identity file strictly: exactly one intact record,
// the magic, this version, a group id of 32 bits, a valid configuration and
// nothing after. Anything else is ErrIdentity.
func DecodeIdentity(b []byte) (Identity, error) {
	bad := func(format string, args ...any) (Identity, error) {
		return Identity{}, fmt.Errorf("%w: %s", ErrIdentity, fmt.Sprintf(format, args...))
	}
	rd := record.NewReader(bytes.NewReader(b), "identity", int64(len(b)))
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
	if len(p) < len(identityMagic) || string(p[:len(identityMagic)]) != identityMagic {
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
	v, ok := uint()
	if !ok || (v != identityVersion && v != identityVersionV1) {
		return bad("version %d (this build reads %d and %d)", v, identityVersionV1, identityVersion)
	}
	g, ok := uint()
	if !ok || g > uint64(^uint32(0)) {
		return bad("group id")
	}
	var node NodeID
	if v == identityVersion {
		l, ok := uint()
		if !ok || l == 0 || l > replication.MaxMemberLen || l > uint64(len(p)) {
			return bad("node id")
		}
		node = NodeID(p[:l])
		p = p[l:]
	}
	n, ok := uint()
	if !ok || n > uint64(len(p)) || n > replication.MaxEncodedConfiguration {
		return bad("configuration length")
	}
	if n != uint64(len(p)) {
		return bad("%d bytes after the configuration", uint64(len(p))-n)
	}
	c, err := replication.DecodeConfiguration(p)
	if err != nil {
		return bad("genesis: %v", err)
	}
	return Identity{Group: replication.GroupID(g), Genesis: c, Node: node}, nil
}

// LoadIdentity reads the identity file beside logPath. found is false when
// there is none.
func LoadIdentity(fsys vfs.FS, logPath string) (id Identity, found bool, err error) {
	fsys = vfs.Or(fsys)
	f, err := fsys.OpenFile(identityPath(logPath), os.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return Identity{}, false, nil
	}
	if err != nil {
		return Identity{}, false, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxIdentityFile+1))
	if err != nil {
		return Identity{}, false, err
	}
	if len(b) > maxIdentityFile {
		return Identity{}, false, fmt.Errorf("%w: the file is over %d bytes", ErrIdentity, maxIdentityFile)
	}
	id, err = DecodeIdentity(b)
	return id, true, err
}

// writeIdentity publishes the identity file durably: a temporary file, fsync,
// rename, directory fsync. A crash before the rename leaves only the
// temporary, which removeIdentityOrphan deletes at the next start.
func writeIdentity(fsys vfs.FS, logPath string, id Identity) error {
	fsys = vfs.Or(fsys)
	b, err := EncodeIdentity(id)
	if err != nil {
		return err
	}
	tmp := identityTmpPath(logPath)
	f, err := fsys.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
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
	if err := fsys.Rename(tmp, identityPath(logPath)); err != nil {
		return err
	}
	return fsys.SyncDir(filepath.Dir(logPath))
}

// removeIdentityOrphan deletes an identity temporary a crash left behind.
func removeIdentityOrphan(fsys vfs.FS, logPath string) error {
	fsys = vfs.Or(fsys)
	err := fsys.Remove(identityTmpPath(logPath))
	switch {
	case err == nil:
		return fsys.SyncDir(filepath.Dir(logPath))
	case errors.Is(err, fs.ErrNotExist):
		return nil
	}
	return err
}

// exists reports whether name exists on fsys.
func exists(fsys vfs.FS, name string) (bool, error) {
	_, err := vfs.Or(fsys).Stat(name)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// Prepare records the identity of a group's first start — cfg's ID, Group and
// genesis (Peers, Bootstrap or Join), beside cfg.LogPath — without starting
// it; a later Start finds it as if its own first start had written it. An
// identity already recorded is checked as Start checks it, and kept. A node
// initializing its data directory prepares every group before any runs
// (cmd/dkvd, audit H1).
func Prepare(cfg Config) error {
	_, err := cfg.identity(cfg.snapshotFiles())
	return err
}
