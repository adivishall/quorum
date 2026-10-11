package wal

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/adivishall/quorum/internal/vfs"
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
// A gap in the sequence is an error. Truncation (S3) deletes only a prefix, so
// a segment missing from the middle was removed from underneath us, and every
// record it held is gone. Replaying the surviving segments would produce
// a state that never existed — a later write applied without the earlier one it
// overwrote. Refusing is the only correct response.
//
// A run of segments missing from the *start* is not a gap, and listSegments
// cannot see it. RecoverFrom checks the start (S3): against the MANIFEST's log
// number when one is recorded, and against the first segment when none is,
// since nothing truncates a log without first recording a log number.
func listSegments(fsys vfs.FS, dir string) (nums []uint64, ignored int, err error) {
	nums, ignored, err = listSegmentsRaw(fsys, dir)
	if err != nil {
		return nil, ignored, err
	}
	if err := checkContiguous(nums); err != nil {
		return nil, ignored, err
	}
	return nums, ignored, nil
}

// listSegmentsRaw lists the segment numbers present in dir, ascending, without
// checking for gaps. Truncation (S3) legitimately deletes a prefix of the
// segments, so a caller that is about to drop everything below a log number
// must list raw, drop, and only then check that what remains is contiguous.
func listSegmentsRaw(fsys vfs.FS, dir string) (nums []uint64, ignored int, err error) {
	entries, err := fsys.ReadDir(dir)
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
	return nums, ignored, nil
}

// checkContiguous refuses a gap in an ascending segment list. Nothing but
// truncation (which removes a whole prefix) ever deletes a segment, so a gap in
// the middle means records were lost, and replaying around it would produce a
// state that never existed.
func checkContiguous(nums []uint64) error {
	for i := 1; i < len(nums); i++ {
		if nums[i] != nums[i-1]+1 {
			return fmt.Errorf(
				"wal: segment sequence has a gap: %s is followed by %s (%d segments missing): %w",
				segmentName(nums[i-1]), segmentName(nums[i]), nums[i]-nums[i-1]-1, ErrCorrupt)
		}
	}
	return nil
}
