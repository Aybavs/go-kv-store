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

// TestPipelinedLargeBatch pushes enough distinct commands in one write to
// force the server's read buffer to refill mid-batch. Each iteration sets its
// own key to its own value — a batch that wrote the same key/value 200 times
// would pass identically whether or not the ownership invariant held, so a
// sample of the keys is re-read afterward and must hold its own value, not
// some other iteration's.
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
