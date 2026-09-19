# BLOOM FILTERS

Status: **Phase 4 — implemented and verified.** Every property stated here is bound to a test
named in `docs/INVARIANTS.md`. Where something is *not* established, this document says so.

Formats: `docs/DESIGN.md` §5. Implementation: `internal/storage/bloom`, integrated in
`internal/storage/sstable`.

---

## 1. The problem

Phase 3 measured its own bottleneck and wrote it down: a lookup cost roughly 1.5 µs per SSTable
consulted, and the SSTable count grew without bound because nothing merged them. A lookup for an
absent key asked **every** file, and each of those questions cost an index binary search plus a
4 KiB block read.

That is the read amplification an LSM tree trades for its cheap writes. Compaction reduces the
number of files; a Bloom filter makes each remaining file cheap to rule out. They are separate
mechanisms for the same cost and Phase 4 needs both.

## 2. What a Bloom filter is for here

One question about one SSTable: *might this file contain any version of this user key?*

| Answer | Meaning | What the read path does |
|---|---|---|
| no | The key is **definitely** absent from this file | Skip the file. No index lookup, no block read. |
| yes | The key **might** be present | Consult the file, exactly as Phase 3 did. |

A false positive costs one wasted block read. A false **negative** would hide a key that is
durably on disk, which is indistinguishable from data loss. The whole design is arranged so that
the "no" path is hard to reach by accident:

- an **absent** filter is not a filter that says no — a file with no filter answers "maybe" to
  everything (`bloom.Filter` zero value), so a Phase 3 file is consulted in full;
- a **malformed** filter is an error, not a no — `bloom.Decode` fails and `sstable.Open` reports
  `ErrCorrupt` rather than quietly treating the file as filterless;
- a filter over **zero keys** is the only case where every answer is legitimately no, and it
  describes a file that holds no keys.

## 3. What is hashed: user keys

The filter covers **user keys**, not internal keys (`docs/DESIGN.md` §5). This is not a detail.

A lookup knows the user key and nothing about which sequence numbers the file happens to hold, so
a filter over internal keys could not be probed at all — every version of a key would be a
different filter entry and the read path has no way to enumerate them. Three versions of one user
key therefore contribute **one** entry.

**Tombstoned keys are in the filter like any other.** The writer adds the user key regardless of
kind. Omitting tombstoned keys would let the file holding a tombstone be skipped, and the lookup
would fall through to an older file holding the value — which is exactly how a deleted key comes
back from the dead. `TestTombstonedKeysAreInTheFilter` and
`TestFilterNeverSkipsAPresentKey` cover it.

## 4. The format

```
k u8 | m u32 | bits[ceil(m/8)]
```

Exactly `docs/DESIGN.md` §5, little-endian, carried in the filter block the SSTable layout has
reserved since Phase 0 and that Phase 3 wrote with length zero.

| Question | Answer |
|---|---|
| Bit count | `m = max(64, n * bitsPerKey)`, `n` = distinct user keys. The 64-bit floor stops a one-key file from getting a ten-bit filter whose false-positive rate is set by rounding; it costs eight bytes. |
| Probes | `k = clamp(round(bitsPerKey * ln2), 1, 30)`. At the default `bitsPerKey = 10` that is **7**. |
| Default bits/key | 10, giving a theoretical false-positive rate near 1%. |
| Probe positions | One 64-bit hash per key split into `h1 = uint32(h)`, `h2 = uint32(h>>32)`, then `g_i = h1 + i*h2 (mod m)` — Kirsch–Mitzenmacher. One real hash, `k` cheap derivations. |
| Empty filter | `n = 0` still produces a well-formed filter, over the empty set. Every probe returns false, which is the true answer for a file with no keys. |
| `filter_length == 0` | "This file carries no filter; consult it directly." What Phase 3 wrote, and what `DisableBloomFilter` still writes. |

### The hash, and why it is that one

**FNV-1a 64 followed by the MurmurHash3 64-bit finalizer.**

FNV-1a alone is unusable in this construction. It is a multiply-xor chain whose high bits barely
mix, and this construction reads the high half as `h2` — so `h2` would be near-constant across
keys and the `k` probes would collapse toward a single bit. The finalizer (three shifts, two
multiplies) is what makes the two halves independent. The measured false-positive rate is
**0.8220%** against a theoretical **0.8194%** (`TestFalsePositiveRateIsNearTheoretical`), which is
the evidence that the mixing is adequate.

It is deliberately **not** a cryptographic hash. A Bloom filter is a performance structure; an
adversary who can choose keys can inflate the false-positive rate, which costs block reads and
cannot cause a wrong answer. Paying for SHA-2 here would buy nothing.

It is deliberately **not** `hash/maphash`, which is seeded randomly per process. The filter is
persisted inside an SSTable and must produce identical bits in every process that ever reads that
file. A per-process seed would make a filter written by one process return false for keys another
process knows are present — a false negative by construction.

### Versioning

There is no version field, because `docs/DESIGN.md` §5 does not specify one and the filter's
identity is already versioned by the file that carries it: the footer magic `DKVSST01` names the
whole SSTable format, this hash included. Changing the hash or the probe construction is a format
change and must bump that magic. See ADR-009.

### Integrity

The filter encoding carries **no checksum of its own**. It does not need one: it lives in a block,
and every block in an SSTable is followed by a `crc32c` that `sstable.Open` verifies
(`docs/DESIGN.md` §4). That checksum is the only thing standing between a flipped bit and a filter
that answers "definitely absent" for a key that is present, which is why it is verified at open
even for a zero-length filter block and why `TestCorruptFilterIsRefusedNotIgnored` damages every
byte of the block in turn.

## 5. Measurements

Development measurements from `TestBloomEffectMeasurement` and `TestFilterSizeMeasurement`, on
go1.27.1, darwin/arm64, 10 CPU. **They are not benchmarks in the sense Phase 5 will mean**:
nothing about the environment is controlled, and no number here may be quoted anywhere as a
result. Phase 5 owns benchmarking methodology.

Same 20,000 keys in 17 SSTables, same 4,000 lookups (2,000 hits and 2,000 misses), no compaction,
10 bits/key:

| Arm | Data blocks read | File consultations skipped | p50 | p95 | p99 |
|---|---|---|---|---|---|
| Filter disabled (Phase 3 behaviour) | 52,329 | 0 | 11.0 µs | 19.2 µs | 22.1 µs |
| Filter enabled | 2,395 | 49,934 | 1.4 µs | 2.6 µs | 3.3 µs |

**95.4% of block reads avoided.** The latency figures move by a similar factor, but the block and
file counts are the result that matters: they are a property of the algorithm, whereas the
microseconds are a property of this laptop's page cache.

Cost, for 20,000 keys in one SSTable:

| | |
|---|---|
| Filter overhead | 25,005 bytes — **exactly 10.00 bits/key**, 2.33% of the file |
| Flush time | ~19–21 ms either way; building the filter is not measurable against writing the file |

The test asserts the measured bits/key against the configured value, so the sizing arithmetic
cannot drift from `docs/DESIGN.md` §5 unnoticed.

## 6. What the tests establish

**Established.**

- **Zero false negatives** (INV-S7) at two levels: over corpora up to 50,000 keys inside the
  package, and through a real SSTable — writer, block checksum, file, reader — for 20,000 keys
  including tombstones and arbitrary byte keys.
- The measured false-positive rate matches the theoretical rate. This is required *alongside* the
  false-negative test, because a filter that always answered "maybe" would pass every
  false-negative test ever written.
- A lookup the filter rejects performs **no block I/O at all**: `skips + blockReads == probes`
  exactly, asserted rather than inferred.
- A malformed or damaged filter is `ErrCorrupt`, never silently downgraded to "no filter".
- A file with no filter (Phase 3, or `DisableBloomFilter`) still serves every key, and skips
  nothing.
- A database holding both filtered and filterless files resolves every key correctly, and a
  compaction across them produces a filtered output.
- Filter bytes are deterministic: the same keys produce byte-identical filters, which they must,
  because the bytes are persisted.

**Not established, and therefore not claimed.**

- **Any statement about performance on hardware other than the machine that ran the test.** §5 is
  development measurement, not a benchmark.
- **A bound on the false-positive rate for adversarially chosen keys.** The hash is not keyed and
  is not meant to be.
- **Filter integrity independent of the block checksum.** Flipping a bit from 1 to 0 inside a
  filter *can* produce a false negative; nothing in the filter itself detects that. The block's
  `crc32c` is what detects it, and `TestExtraBitsOnlyAddFalsePositives` documents which direction
  is and is not safe.

## 7. Limitations

| Limitation | Removed in |
|---|---|
| The filter is consulted per file; there is no index over files, so a lookup still asks every live file's filter | not planned — with compaction bounding the file count, the filter answers are cheap |
| No partitioned or ribbon filters, no per-level bits/key tuning | deferred; a measurement would have to justify them |
| `bitsPerKey` is global, not per level | deferred. Deep levels are read more often per byte and could afford more bits; that is a Phase 5 question with a benchmark attached |
| Filters are rebuilt from scratch on every compaction | inherent to writing a new file; the cost is not measurable against writing the data (§5) |
