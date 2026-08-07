package command

// ReplyKind identifies which RESP reply type a command produced. The command
// layer deliberately does not know RESP framing; the server encodes these.
type ReplyKind int

const (
	ReplySimple ReplyKind = iota
	ReplyError
	ReplyInt
	ReplyBulk
	ReplyNullBulk
	ReplyArray
)

// Reply is the plain-Go result of executing a command.
type Reply struct {
	Kind  ReplyKind
	Str   string
	Int   int64
	Array []Reply
}

func Simple(s string) Reply     { return Reply{Kind: ReplySimple, Str: s} }
func Err(s string) Reply        { return Reply{Kind: ReplyError, Str: s} }
func Int(n int64) Reply         { return Reply{Kind: ReplyInt, Int: n} }
func Bulk(s string) Reply       { return Reply{Kind: ReplyBulk, Str: s} }
func NullBulk() Reply           { return Reply{Kind: ReplyNullBulk} }
func Array(items []Reply) Reply { return Reply{Kind: ReplyArray, Array: items} }
