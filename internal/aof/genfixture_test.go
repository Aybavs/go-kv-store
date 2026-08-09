package aof

import (
	"os"
	"testing"
	"time"
)

// Run once, by hand, to produce the committed fixture. Not part of the suite.
func TestGenerateV1Fixture(t *testing.T) {
	if os.Getenv("KV_WRITE_FIXTURE") != "1" {
		t.Skip("set KV_WRITE_FIXTURE=1 to regenerate")
	}
	path := "testdata/v1-format.aof"
	// Through OpenFile, so the fixture carries the real 16-byte header rather
	// than one this test invented.
	l, res, err := OpenFile(path, Always, time.Now(), noopApplier{}, func(err error) { t.Fatalf("fatal: %v", err) })
	if err != nil {
		t.Fatal(err)
	}
	if res.Records != 0 {
		t.Fatalf("the fixture path already holds %d records; delete it first", res.Records)
	}
	deadline := time.UnixMilli(1893456000000) // 2030-01-01T00:00:00Z, far future
	for _, r := range []Record{
		DeriveSet("plain", "value", time.Time{}, false),
		DeriveSet("binary\r\n\x00", "a\r\nb", time.Time{}, false),
		DeriveSet("with-ttl", "42", deadline, true),
		DeriveSet("empty", "", time.Time{}, false),
		DeriveDel([]string{"gone-1", "gone-2", "gone-3"}),
	} {
		if _, err := l.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", path)
}

type noopApplier struct{}

func (noopApplier) Set(string, string, time.Time, bool) {}
func (noopApplier) Delete(string) bool                  { return false }
