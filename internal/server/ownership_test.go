package server

import (
	"io"
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

// TestFragmentedScanAndPipelinedReplyAlignment covers two independent TCP
// delivery shapes: one-byte request fragmentation and a mixed reply pipeline.
func TestFragmentedScanAndPipelinedReplyAlignment(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)
	c.send(t, "SET", "a", "v")
	if got := c.readReply(t); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}

	frame := []byte("*6\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$1\r\n*\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n")
	for _, b := range frame {
		if _, err := c.conn.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	wantScan := "*2\r\n$1\r\n0\r\n*1\r\n$1\r\na\r\n"
	got := make([]byte, len(wantScan))
	if _, err := io.ReadFull(c.br, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != wantScan {
		t.Fatalf("fragmented SCAN = %q, want %q", got, wantScan)
	}

	payload := "*2\r\n$4\r\nSCAN\r\n$1\r\n0\r\n" +
		"*1\r\n$6\r\nDBSIZE\r\n" +
		"*1\r\n$4\r\nPING\r\n"
	if _, err := c.conn.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	want := "*2\r\n$1\r\n0\r\n*1\r\n$1\r\na\r\n:1\r\n+PONG\r\n"
	all := make([]byte, len(want))
	if _, err := io.ReadFull(c.br, all); err != nil {
		t.Fatal(err)
	}
	if string(all) != want {
		t.Fatalf("pipeline = %q, want %q", all, want)
	}
}

// Enough distinct commands in one write to refill the read buffer mid-batch.
// Each iteration uses its own key and value: repeating one pair would pass
// whether or not the ownership invariant held.
func TestPipelinedLargeBatch(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)

	const n = 200
	keys := make([]string, n)
	values := make([]string, n)
	var payload []byte
	for i := 0; i < n; i++ {
		key := "k" + itoa(i)
		value := "v" + itoa(i)
		keys[i] = key
		values[i] = value

		payload = append(payload, "*3\r\n$3\r\nSET\r\n"...)
		payload = append(payload, '$')
		payload = append(payload, itoa(len(key))...)
		payload = append(payload, '\r', '\n')
		payload = append(payload, key...)
		payload = append(payload, '\r', '\n')
		payload = append(payload, '$')
		payload = append(payload, itoa(len(value))...)
		payload = append(payload, '\r', '\n')
		payload = append(payload, value...)
		payload = append(payload, '\r', '\n')
	}
	if _, err := c.conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	for i := 0; i < n; i++ {
		if got := c.readReply(t); got != "+OK" {
			t.Fatalf("reply %d: got %q, want +OK", i, got)
		}
	}

	for _, i := range []int{0, 1, 2, 50, 99, 100, 150, 199} {
		c.send(t, "GET", keys[i])
		want := "$" + itoa(len(values[i])) + "\r\n" + values[i]
		if got := c.readReply(t); got != want {
			t.Fatalf("GET %s: got %q, want %q", keys[i], got, want)
		}
	}
}
