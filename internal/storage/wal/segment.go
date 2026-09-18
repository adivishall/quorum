package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Segment files are named %06d.log and numbered from 1 (docs/DESIGN.md §6).
// Six digits allow 999,999 segments, which at the default 16 MiB segment size
// is about 15 TiB of log — far past the point where the absence of log
// truncation (Phase 4) would have become the real problem.
const (
	segmentSuffix    = ".log"
	segmentDigits    = 6
	firstSegment     = uint64(1)
	maxSegmentNumber = uint64(999999)
)

// segmentName renders a segment number as its file name.
func segmentName(n uint64) string {
	return fmt.Sprintf("%0*d%s", segmentDigits, n, segmentSuffix)
}

// segmentPath renders a segment number as a full path inside dir.
func segmentPath(dir string, n uint64) string {
	return filepath.Join(dir, segmentName(n))
}

// parseSegmentName parses a segment file name.
//
// The match is exact: precisely six decimal digits followed by ".log". Anything
// else is not a segment. Being strict here means a stray file cannot be
// mistaken for log data, and being tolerant in listSegments (which ignores
// non-matches) means an editor swap file or a .DS_Store cannot stop the
// database from opening.
func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	digits := strings.TrimSuffix(name, segmentSuffix)
	if len(digits) != segmentDigits {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// listSegments returns the segment numbers present in dir, in ascending order,
// along with the number of entries that were ignored because they are not
// segment files.
//
// A gap in the sequence is an error. Nothing in Quorum deletes a WAL segment yet,
// so a missing segment means a segment was removed from underneath us, and
// every record it held is gone. Replaying the surviving segments would produce
// a state that never existed — a later write applied without the earlier one it
// overwrote. Refusing is the only correct response.
//
// Note the asymmetry: a gap in the middle is detectable, a run of segments
// missing from the *start* is not, because nothing records which segment number
// the log begins at. That metadata arrives with the MANIFEST's log number in
// Phase 4. Until then, this is a documented limitation rather than a guarantee.
func listSegments(dir string) (nums []uint64, ignored int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}

	for _, e := range entries {
		if e.IsDir() {
			ignored++
			continue
		}
		n, ok := parseSegmentName(e.Name())
		if !ok {
			ignored++
			continue
		}
		nums = append(nums, n)
	}

	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	for i := 1; i < len(nums); i++ {
		if nums[i] != nums[i-1]+1 {
			return nil, ignored, fmt.Errorf(
				"wal: segment sequence has a gap: %s is followed by %s (%d segments missing): %w",
				segmentName(nums[i-1]), segmentName(nums[i]), nums[i]-nums[i-1]-1, ErrCorrupt)
		}
	}

	return nums, ignored, nil
}

// syncDir fsyncs a directory so that file creations and renames within it are
// durable. Creating a file is not durable until its parent directory is
// flushed: after a crash the file can exist in the page cache but be absent
// from the directory, which for a freshly rotated WAL segment means the newest
// records vanish while the older segments survive.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
