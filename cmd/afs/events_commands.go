package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/redis/agent-filesystem/internal/controlplane"
)

type eventsFlags struct {
	workspace string
	sessionID string
	kinds     []string
	path      string
	limit     int
	since     string
}

func cmdEvents(args []string) error {
	for _, a := range args[1:] {
		if isHelpArg(a) {
			fmt.Fprint(os.Stderr, eventsUsageText(filepath.Base(os.Args[0])))
			return nil
		}
	}

	flags, err := parseEventsArgs(args[1:])
	if err != nil {
		return fmt.Errorf("%s\n\n%s", err, eventsUsageText(filepath.Base(os.Args[0])))
	}

	ctx := context.Background()
	cfg, service, closeFn, err := openAFSControlPlane(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	selection, err := resolveWorkspaceSelectionFromControlPlane(ctx, cfg, service, flags.workspace)
	if err != nil {
		return err
	}

	req := controlplane.EventsListRequest{
		Kinds:     flags.kinds,
		SessionID: flags.sessionID,
		Path:      flags.path,
		Limit:     flags.limit,
		Reverse:   true,
	}
	if flags.since != "" {
		sinceID, err := resolveSinceCursor(flags.since)
		if err != nil {
			return err
		}
		req.Since = sinceID
	}
	resp, err := service.ListEvents(ctx, selection.Name, req)
	if err != nil {
		return err
	}

	printEventEntries(selection.Name, resp.Entries)
	return nil
}

func parseEventsArgs(args []string) (eventsFlags, error) {
	flags := eventsFlags{limit: 100}
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--workspace", a == "-w":
			if i+1 >= len(args) {
				return flags, fmt.Errorf("--workspace requires a value")
			}
			i++
			flags.workspace = args[i]
		case strings.HasPrefix(a, "--workspace="):
			flags.workspace = strings.TrimPrefix(a, "--workspace=")
		case a == "--session":
			if i+1 >= len(args) {
				return flags, fmt.Errorf("--session requires a value")
			}
			i++
			flags.sessionID = args[i]
		case strings.HasPrefix(a, "--session="):
			flags.sessionID = strings.TrimPrefix(a, "--session=")
		case a == "--kind":
			if i+1 >= len(args) {
				return flags, fmt.Errorf("--kind requires a value")
			}
			i++
			flags.kinds = append(flags.kinds, splitCSV(args[i])...)
		case strings.HasPrefix(a, "--kind="):
			flags.kinds = append(flags.kinds, splitCSV(strings.TrimPrefix(a, "--kind="))...)
		case a == "--path":
			if i+1 >= len(args) {
				return flags, fmt.Errorf("--path requires a value")
			}
			i++
			flags.path = args[i]
		case strings.HasPrefix(a, "--path="):
			flags.path = strings.TrimPrefix(a, "--path=")
		case a == "--limit":
			if i+1 >= len(args) {
				return flags, fmt.Errorf("--limit requires a value")
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n <= 0 {
				return flags, fmt.Errorf("--limit must be a positive integer")
			}
			flags.limit = n
		case strings.HasPrefix(a, "--limit="):
			n, err := strconv.Atoi(strings.TrimPrefix(a, "--limit="))
			if err != nil || n <= 0 {
				return flags, fmt.Errorf("--limit must be a positive integer")
			}
			flags.limit = n
		case a == "--since":
			if i+1 >= len(args) {
				return flags, fmt.Errorf("--since requires a value")
			}
			i++
			flags.since = args[i]
		case strings.HasPrefix(a, "--since="):
			flags.since = strings.TrimPrefix(a, "--since=")
		case strings.HasPrefix(a, "-"):
			return flags, fmt.Errorf("unknown flag %q", a)
		default:
			return flags, fmt.Errorf("unexpected argument %q", a)
		}
	}
	return flags, nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// resolveSinceCursor accepts either a Redis Stream ID (e.g. "1700000000-0")
// or a duration like "1h" / "30m" which is converted to a ms-second stream
// cursor. Returns a value safe to pass as Since in an EventsListRequest.
func resolveSinceCursor(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if strings.ContainsAny(raw, "-") {
		// Looks like a Redis stream ID; pass through unchanged.
		return raw, nil
	}
	dur, err := time.ParseDuration(raw)
	if err != nil {
		return "", fmt.Errorf("invalid --since value %q (want a stream id or a duration like 1h)", raw)
	}
	cursorMs := time.Now().UTC().Add(-dur).UnixMilli()
	return strconv.FormatInt(cursorMs, 10) + "-0", nil
}

func printEventEntries(workspace string, entries []controlplane.EventRow) {
	header := "events · workspace " + workspace
	fmt.Println(clr(ansiBold, header))
	if len(entries) == 0 {
		fmt.Println(clr(ansiDim, "  (no events recorded)"))
		return
	}
	for _, row := range entries {
		printEventRow(row)
	}
}

func printEventRow(row controlplane.EventRow) {
	when := row.OccurredAt
	if when == "" {
		when = row.ID
	} else if t, err := time.Parse(time.RFC3339, when); err == nil {
		when = t.Local().Format("15:04:05")
	}
	kind := padLeft(row.Kind, 10)
	op := padLeft(row.Op, 8)
	var detail string
	switch row.Kind {
	case controlplane.EventKindFile:
		path := row.Path
		if row.PrevPath != "" {
			path = row.PrevPath + " → " + path
		}
		detail = fmt.Sprintf("%s %s", formatDeltaBytes(row.DeltaBytes), path)
	case controlplane.EventKindCheckpoint:
		detail = row.CheckpointID
	case controlplane.EventKindSession:
		detail = shortSession(row.SessionID)
		if host := row.Hostname; host != "" {
			detail += " @ " + host
		}
	case controlplane.EventKindWorkspace:
		detail = ""
		if source := row.Extras["source_workspace"]; source != "" {
			detail = "from " + source
		}
	case controlplane.EventKindProcess:
		if code := row.Extras["exit_code"]; code != "" {
			detail = "exit=" + code
		}
		if argv := row.Extras["argv"]; argv != "" {
			if detail != "" {
				detail = argv + " (" + detail + ")"
			} else {
				detail = argv
			}
		}
	}
	fmt.Printf("  %s %s %s  %s\n",
		clr(ansiDim, when),
		clr(ansiBold, kind),
		op,
		detail,
	)
}

func eventsUsageText(bin string) string {
	return brandHeaderString() + fmt.Sprintf(`Usage:
  %s events [flags]

Show the unified workspace history — lifecycle events (workspace, session,
checkpoint, process) and file operations in a single stream.

Flags:
  --workspace, -w <name>   Override the current workspace
  --session <id>           Filter to a single session
  --kind <list>            Filter kinds (comma-separated): workspace, session,
                           checkpoint, process, file. Default: all kinds.
  --path <path>            Filter file ops to an exact workspace-relative path
  --since <ref>            Stream ID or duration (e.g. 1h, 30m) to start at
  --limit <n>              Max rows to return (default 100, max 1000)

Examples:
  %s events --limit 20
  %s events --kind checkpoint,session
  %s events --kind file --path /src/main.go
  %s events --session sess_abcdef --since 1h
`, bin, bin, bin, bin, bin)
}
