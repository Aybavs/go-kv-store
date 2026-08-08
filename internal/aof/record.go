// Package aof implements the append-only file: the canonical effect records
// that make crash recovery possible, their encoding, and the file they live in.
//
// The log records the resulting durable state mutation, never the client
// command. That distinction is the whole design, and ADR-0004 carries the
// counterexample: a command log makes recovery depend on prior state, so any
// read-modify-write command silently breaks it. An effect record is independent
// of every record before it.
//
// One consequence is worth stating here because it removes work rather than
// adding it: active expiration never needs to append anything. A key that
// expired is already absent on replay, by the rules in replay.go.
package aof

import "time"

// Kind is the verb of a record. The vocabulary is deliberately two shapes; it
// is not claimed to be complete forever, and extending it is a decision rather
// than an accident.
type Kind int

const (
	KindSet Kind = iota
	KindDel
)

// Record is one canonical effect.
//
// A Del carries every key of the originating command, because one complete
// record is one recovery atomicity unit: recovery restores a prefix and stops
// at the first incomplete record, so splitting DEL k1 k2 k3 into three records
// would let a crash restore a partial multi-key delete.
type Record struct {
	Kind Kind

	// KindSet
	Key        string
	Value      string
	ExpireAtMS int64 // absolute Unix milliseconds; meaningful only with HasExpiry
	HasExpiry  bool

	// KindDel
	Keys []string
}

// DeriveSet builds the effect for any mutation that leaves a key holding a
// value: SET itself, but also EXPIRE and PERSIST, which are expressed as a SET
// of the value the key already holds. Deadlines are absolute, so a record read
// back weeks later means what it meant when it was written.
func DeriveSet(key, value string, deadline time.Time, hasTTL bool) Record {
	r := Record{Kind: KindSet, Key: key, Value: value, HasExpiry: hasTTL}
	if hasTTL {
		r.ExpireAtMS = deadline.UnixMilli()
	}
	return r
}

// DeriveDel builds the effect for a delete. The keys are copied: the caller's
// slice belongs to the command layer and does not outlive the request, while a
// record may sit in the write buffer well past it.
func DeriveDel(keys []string) Record {
	owned := make([]string, len(keys))
	copy(owned, keys)
	return Record{Kind: KindDel, Keys: owned}
}
