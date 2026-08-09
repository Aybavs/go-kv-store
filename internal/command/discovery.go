package command

import (
	"errors"
	"strconv"
	"strings"

	"github.com/aybavs/go-kv-store/internal/engine"
)

const (
	defaultScanCount    uint64 = 10
	errInvalidCursor           = "ERR invalid cursor"
	errScanSessionLimit        = "ERR scan session limit reached"
	errScanMatchChanged        = "ERR scan MATCH cannot change during iteration"
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
	req, bad := parseScan(args)
	if bad != nil {
		return *bad
	}
	page, err := e.Scan(req)
	if err != nil {
		return scanError(err)
	}
	items := make([]Reply, 0, len(page.Keys))
	for _, key := range page.Keys {
		items = append(items, Bulk(key))
	}
	return Array([]Reply{
		Bulk(strconv.FormatUint(page.Cursor, 10)),
		Array(items),
	})
}

func scanError(err error) Reply {
	switch {
	case errors.Is(err, engine.ErrInvalidCursor):
		return Err(errInvalidCursor)
	case errors.Is(err, engine.ErrScanSessionLimit):
		return Err(errScanSessionLimit)
	case errors.Is(err, engine.ErrScanMatchChanged):
		return Err(errScanMatchChanged)
	default:
		return mutationError(err)
	}
}

func parseScan(args [][]byte) (engine.ScanRequest, *Reply) {
	req := engine.ScanRequest{Count: defaultScanCount}
	cursorText := string(args[1])
	if strings.HasPrefix(cursorText, "+") {
		cursorText = cursorText[1:]
		if cursorText == "" {
			return engine.ScanRequest{}, errReply(errInvalidCursor)
		}
	}
	cursor, err := strconv.ParseUint(cursorText, 10, 64)
	if err != nil {
		return engine.ScanRequest{}, errReply(errInvalidCursor)
	}
	req.Cursor = cursor
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			if i+1 >= len(args) {
				return engine.ScanRequest{}, errReply(errSyntax)
			}
			req.Pattern = string(args[i+1])
			req.PatternSet = true
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return engine.ScanRequest{}, errReply(errSyntax)
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return engine.ScanRequest{}, errReply(errNotAnInteger)
			}
			if n <= 0 {
				return engine.ScanRequest{}, errReply(errSyntax)
			}
			req.Count = uint64(n)
			i++
		default:
			return engine.ScanRequest{}, errReply(errSyntax)
		}
	}
	return req, nil
}
