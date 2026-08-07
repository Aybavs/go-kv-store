package server

import (
	"testing"
)

// TestPipelinedSingleWrite sends several commands in one client write. The
// server must handle every command correctly regardless of how the bytes are
// delivered — TCP segmentation is not under application control, so this test
// asserts behaviour, not segmentation.
func TestPipelinedSingleWrite(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)

	payload := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$3\r\none\r\n" +
		"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$3\r\ntwo\r\n" +
		"*2\r\n$3\r\nGET\r\n$1\r\na\r\n" +
		"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n"

	if _, err := c.conn.Write([]byte(payload)); err != nil {
		t.Fatalf("write: %v", err)
	}

	want := []string{"+OK", "+OK", "$3\r\none", "$3\r\ntwo"}
	for i, w := range want {
		if got := c.readReply(t); got != w {
			t.Fatalf("reply %d: got %q, want %q", i, got, w)
		}
	}
}

// TestPipelinedLargeBatch pushes enough commands in one write to force the
// server's read buffer to refill mid-batch.
func TestPipelinedLargeBatch(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)

	const n = 200
	var payload []byte
	for i := 0; i < n; i++ {
		payload = append(payload, []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\nvvvv\r\n")...)
	}
	if _, err := c.conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	for i := 0; i < n; i++ {
		if got := c.readReply(t); got != "+OK" {
			t.Fatalf("reply %d: got %q, want +OK", i, got)
		}
	}
}
