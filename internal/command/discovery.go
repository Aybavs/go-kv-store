package command

import (
	"strconv"
	"strings"

	"github.com/aybavs/go-kv-store/internal/engine"
)

const (
	defaultScanCount uint64 = 10
	errInvalidCursor        = "ERR invalid cursor"
)

func cmdKeys(e *engine.Engine, args [][]byte) Reply {
	keys := e.Keys(string(args[1]))
	items := make([]Reply, 0, len(keys))
	for _, key := range keys {
		items = append(items, Bulk(key))
	}
	return Array(items)
}

func cmdDBSize(e *engine.Engine, _ [][]byte) Reply {
	return Int(int64(e.DBSize()))
}

func cmdScan(e *engine.Engine, args [][]byte) Reply {
	cursor, pattern, count, bad := parseScan(args)
	if bad != nil {
		return *bad
	}
	page := e.Scan(cursor, pattern, count)
	items := make([]Reply, 0, len(page.Keys))
	for _, key := range page.Keys {
		items = append(items, Bulk(key))
	}
	return Array([]Reply{
		Bulk(strconv.FormatUint(page.Cursor, 10)),
		Array(items),
	})
}

func parseScan(args [][]byte) (uint64, string, uint64, *Reply) {
	cursorText := string(args[1])
	if strings.HasPrefix(cursorText, "+") {
		cursorText = cursorText[1:]
		if cursorText == "" {
			return 0, "", 0, errReply(errInvalidCursor)
		}
	}
	cursor, err := strconv.ParseUint(cursorText, 10, 64)
	if err != nil {
		return 0, "", 0, errReply(errInvalidCursor)
	}
	pattern, count := "*", defaultScanCount
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			if i+1 >= len(args) {
				return 0, "", 0, errReply(errSyntax)
			}
			pattern = string(args[i+1])
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return 0, "", 0, errReply(errSyntax)
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return 0, "", 0, errReply(errNotAnInteger)
			}
			if n <= 0 {
				return 0, "", 0, errReply(errSyntax)
			}
			count = uint64(n)
			i++
		default:
			return 0, "", 0, errReply(errSyntax)
		}
	}
	return cursor, pattern, count, nil
}
