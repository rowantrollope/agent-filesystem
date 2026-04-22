package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Unified workspace event stream. See plans/event-history-merge.md.
//
// One row per event, wide schema, `kind` discriminates between lifecycle
// events (workspace/session/checkpoint/process) and file ops (file). Written
// in parallel with the legacy `workspace:audit` and `workspace:changelog`
// streams during the dual-write phase so we can validate and switch readers
// incrementally.

const (
	EventKindWorkspace  = "workspace"
	EventKindSession    = "session"
	EventKindCheckpoint = "checkpoint"
	EventKindProcess    = "process"
	EventKindFile       = "file"
)

// eventsStreamMaxLen is the soft cap passed to XADD MAXLEN ~ per workspace.
// Covers both lifecycle (~dozens) and file ops (10³–10⁵) volumes.
const eventsStreamMaxLen = 200000

// EventEntry is the wire shape for one row of the unified events stream.
// Empty fields are elided at write time, same as the legacy changelog.
type EventEntry struct {
	Kind         string            `json:"kind"`
	Op           string            `json:"op"`
	Source       string            `json:"source,omitempty"`
	Actor        string            `json:"actor,omitempty"`
	SessionID    string            `json:"session_id,omitempty"`
	User         string            `json:"user,omitempty"`
	Label        string            `json:"label,omitempty"`
	AgentVersion string            `json:"agent_version,omitempty"`
	Hostname     string            `json:"hostname,omitempty"`
	Path         string            `json:"path,omitempty"`
	PrevPath     string            `json:"prev_path,omitempty"`
	SizeBytes    int64             `json:"size_bytes,omitempty"`
	DeltaBytes   int64             `json:"delta_bytes,omitempty"`
	ContentHash  string            `json:"content_hash,omitempty"`
	PrevHash     string            `json:"prev_hash,omitempty"`
	Mode         uint32            `json:"mode,omitempty"`
	CheckpointID string            `json:"checkpoint_id,omitempty"`
	Extras       map[string]string `json:"extras,omitempty"`
}

// fields returns the XADD field map for this entry, with zero-valued fields
// elided.
func (e EventEntry) fields(tsMs int64) map[string]any {
	fields := map[string]any{
		"kind":  e.Kind,
		"op":    e.Op,
		"ts_ms": strconv.FormatInt(tsMs, 10),
	}
	if e.Source != "" {
		fields["source"] = e.Source
	}
	if e.Actor != "" {
		fields["actor"] = e.Actor
	}
	if e.SessionID != "" {
		fields["session_id"] = e.SessionID
	}
	if e.User != "" {
		fields["user"] = e.User
	}
	if e.Label != "" {
		fields["label"] = e.Label
	}
	if e.AgentVersion != "" {
		fields["agent_version"] = e.AgentVersion
	}
	if e.Hostname != "" {
		fields["hostname"] = e.Hostname
	}
	if e.Path != "" {
		fields["path"] = e.Path
	}
	if e.PrevPath != "" {
		fields["prev_path"] = e.PrevPath
	}
	if e.SizeBytes != 0 {
		fields["size_bytes"] = strconv.FormatInt(e.SizeBytes, 10)
	}
	if e.DeltaBytes != 0 {
		fields["delta_bytes"] = strconv.FormatInt(e.DeltaBytes, 10)
	}
	if e.ContentHash != "" {
		fields["content_hash"] = e.ContentHash
	}
	if e.PrevHash != "" {
		fields["prev_hash"] = e.PrevHash
	}
	if e.Mode != 0 {
		fields["mode"] = strconv.FormatUint(uint64(e.Mode), 10)
	}
	if e.CheckpointID != "" {
		fields["checkpoint_id"] = e.CheckpointID
	}
	if len(e.Extras) > 0 {
		if raw, err := json.Marshal(e.Extras); err == nil {
			fields["extras"] = string(raw)
		}
	}
	return fields
}

// eventsStreamKey returns the Redis stream key for the unified events stream
// of the given workspace.
func eventsStreamKey(workspace string) string {
	return fmt.Sprintf("afs:{%s}:workspace:events", workspace)
}

// EventsStreamKey is the exported variant used by tests and integration code.
func EventsStreamKey(workspace string) string { return eventsStreamKey(workspace) }

// enqueueEvents queues XADD commands for the given entries on the caller-
// provided pipeliner.
func enqueueEvents(ctx context.Context, pipe redis.Pipeliner, storageID string, entries []EventEntry) {
	if len(entries) == 0 || pipe == nil {
		return
	}
	streamKey := eventsStreamKey(storageID)
	tsMs := time.Now().UTC().UnixMilli()
	for _, entry := range entries {
		if entry.Kind == "" {
			// Kind is required; skip malformed entries rather than XADDing junk.
			continue
		}
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: eventsStreamMaxLen,
			Approx: true,
			Values: entry.fields(tsMs),
		})
	}
}

// writeEvents is the non-transactional writer used by call sites that don't
// hold a pipeliner (most lifecycle ops). Failures are logged but never
// surfaced to the caller — observability must not block correctness.
func writeEvents(ctx context.Context, rdb redis.Cmdable, storageID string, entries []EventEntry) {
	if len(entries) == 0 || rdb == nil {
		return
	}
	pipe := rdb.Pipeline()
	enqueueEvents(ctx, pipe, storageID, entries)
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("events write failed",
			"workspace", storageID,
			"entries", len(entries),
			"err", err)
	}
}

// WriteEvents is the exported non-transactional writer. Used by package-
// external call sites (e.g. the agent's sync daemon).
func WriteEvents(ctx context.Context, rdb redis.Cmdable, storageID string, entries []EventEntry) {
	writeEvents(ctx, rdb, storageID, entries)
}

// EventsListRequest parameterizes a unified events read. All fields optional.
type EventsListRequest struct {
	Kinds     []string // filter by kind (e.g. "file", "checkpoint"); empty = all
	SessionID string   // filter by session id
	Path      string   // filter by exact path (file ops only)
	Since     string   // entry ID (exclusive when non-empty unless Reverse)
	Until     string   // entry ID (inclusive)
	Limit     int      // hard cap, default 100, max 1000
	Reverse   bool     // newest-first via XREVRANGE
}

// EventsListResponse wraps a page of unified event rows.
type EventsListResponse struct {
	Entries    []EventRow `json:"entries"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// EventRow is the JSON shape of one unified event.
type EventRow struct {
	ID           string            `json:"id"`
	OccurredAt   string            `json:"occurred_at,omitempty"`
	Kind         string            `json:"kind"`
	Op           string            `json:"op"`
	Source       string            `json:"source,omitempty"`
	Actor        string            `json:"actor,omitempty"`
	SessionID    string            `json:"session_id,omitempty"`
	User         string            `json:"user,omitempty"`
	Label        string            `json:"label,omitempty"`
	AgentVersion string            `json:"agent_version,omitempty"`
	Hostname     string            `json:"hostname,omitempty"`
	Path         string            `json:"path,omitempty"`
	PrevPath     string            `json:"prev_path,omitempty"`
	SizeBytes    int64             `json:"size_bytes,omitempty"`
	DeltaBytes   int64             `json:"delta_bytes,omitempty"`
	ContentHash  string            `json:"content_hash,omitempty"`
	PrevHash     string            `json:"prev_hash,omitempty"`
	Mode         uint32            `json:"mode,omitempty"`
	CheckpointID string            `json:"checkpoint_id,omitempty"`
	Extras       map[string]string `json:"extras,omitempty"`
}

func eventRowFromMessage(msg redis.XMessage) EventRow {
	row := EventRow{ID: msg.ID}
	get := func(key string) string {
		if v, ok := msg.Values[key]; ok {
			return fmt.Sprint(v)
		}
		return ""
	}
	if raw := get("ts_ms"); raw != "" {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
			row.OccurredAt = time.UnixMilli(ms).UTC().Format(time.RFC3339)
		}
	}
	row.Kind = get("kind")
	row.Op = get("op")
	row.Source = get("source")
	row.Actor = get("actor")
	row.SessionID = get("session_id")
	row.User = get("user")
	row.Label = get("label")
	row.AgentVersion = get("agent_version")
	row.Hostname = get("hostname")
	row.Path = get("path")
	row.PrevPath = get("prev_path")
	if raw := get("size_bytes"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			row.SizeBytes = n
		}
	}
	if raw := get("delta_bytes"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			row.DeltaBytes = n
		}
	}
	row.ContentHash = get("content_hash")
	row.PrevHash = get("prev_hash")
	if raw := get("mode"); raw != "" {
		if n, err := strconv.ParseUint(raw, 10, 32); err == nil {
			row.Mode = uint32(n)
		}
	}
	row.CheckpointID = get("checkpoint_id")
	if raw := get("extras"); raw != "" {
		var extras map[string]string
		if err := json.Unmarshal([]byte(raw), &extras); err == nil && len(extras) > 0 {
			row.Extras = extras
		}
	}
	return row
}

// ListEvents reads a page of entries from the unified workspace events stream.
// Filters (kind, session, path) are applied in-memory after fetch; the caller
// can over-fetch by setting a higher Limit if a strict filter is expected to
// skip many rows.
func (s *Store) ListEvents(ctx context.Context, storageID string, req EventsListRequest) (EventsListResponse, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	stream := eventsStreamKey(storageID)
	start := req.Since
	end := req.Until
	if start == "" {
		start = "-"
	}
	if end == "" {
		end = "+"
	}
	// Over-fetch when an in-memory filter is in play so the page isn't starved.
	fetch := int64(limit)
	filtering := len(req.Kinds) > 0 || req.SessionID != "" || req.Path != ""
	if filtering {
		fetch = int64(limit) * 4
		if fetch > 4000 {
			fetch = 4000
		}
	}
	var (
		msgs []redis.XMessage
		err  error
	)
	if req.Reverse {
		msgs, err = s.rdb.XRevRangeN(ctx, stream, end, start, fetch).Result()
	} else {
		msgs, err = s.rdb.XRangeN(ctx, stream, start, end, fetch).Result()
	}
	if err != nil {
		return EventsListResponse{}, err
	}
	kindSet := make(map[string]struct{}, len(req.Kinds))
	for _, k := range req.Kinds {
		if k = strings.TrimSpace(k); k != "" {
			kindSet[k] = struct{}{}
		}
	}
	entries := make([]EventRow, 0, len(msgs))
	for _, m := range msgs {
		row := eventRowFromMessage(m)
		if len(kindSet) > 0 {
			if _, ok := kindSet[row.Kind]; !ok {
				continue
			}
		}
		if req.SessionID != "" && row.SessionID != req.SessionID {
			continue
		}
		if req.Path != "" && row.Path != req.Path {
			continue
		}
		entries = append(entries, row)
		if len(entries) >= limit {
			break
		}
	}
	resp := EventsListResponse{Entries: entries}
	if len(entries) > 0 {
		resp.NextCursor = entries[len(entries)-1].ID
	}
	return resp, nil
}

// auditEventEntry maps a legacy Store.Audit() call to an EventEntry in the
// unified stream. Returns (entry, ok) — `ok` is false for audit ops we do not
// know how to translate; the caller should skip the dual-write in that case.
// Mapping is the single source of truth for lifecycle → events translation.
func auditEventEntry(op string, extra map[string]any) (EventEntry, bool) {
	entry := EventEntry{Actor: "afs"}
	stringExtra := func(key string) string {
		if v, ok := extra[key]; ok {
			return strings.TrimSpace(fmt.Sprint(v))
		}
		return ""
	}
	switch op {
	case "workspace_create":
		entry.Kind = EventKindWorkspace
		entry.Op = "create"
	case "workspace_update":
		entry.Kind = EventKindWorkspace
		entry.Op = "update"
	case "workspace_fork":
		entry.Kind = EventKindWorkspace
		entry.Op = "fork"
	case "import":
		entry.Kind = EventKindWorkspace
		entry.Op = "import"
	case "session_start":
		entry.Kind = EventKindSession
		entry.Op = "start"
	case "session_close":
		entry.Kind = EventKindSession
		entry.Op = "close"
	case "session_stale":
		entry.Kind = EventKindSession
		entry.Op = "stale"
	case "save":
		entry.Kind = EventKindCheckpoint
		entry.Op = "save"
		entry.CheckpointID = stringExtra("savepoint")
	case "checkpoint_restore":
		entry.Kind = EventKindCheckpoint
		entry.Op = "restore"
		entry.CheckpointID = stringExtra("checkpoint")
	case "run_start":
		entry.Kind = EventKindProcess
		entry.Op = "start"
	case "run_exit":
		entry.Kind = EventKindProcess
		entry.Op = "exit"
	default:
		return EventEntry{}, false
	}
	// Promote well-known fields out of the extras bag so filtering and UI
	// rendering stay flat; everything else is round-tripped in extras.
	extras := map[string]string{}
	for key, value := range extra {
		str := strings.TrimSpace(fmt.Sprint(value))
		if str == "" {
			continue
		}
		switch key {
		case "session_id":
			entry.SessionID = str
		case "client_kind":
			if entry.Extras == nil {
				entry.Extras = map[string]string{}
			}
			extras[key] = str
		case "hostname":
			entry.Hostname = str
		case "checkpoint", "savepoint":
			if entry.CheckpointID == "" {
				entry.CheckpointID = str
			}
			extras[key] = str
		default:
			extras[key] = str
		}
	}
	if len(extras) > 0 {
		entry.Extras = extras
	}
	return entry, true
}

// changeEventEntry adapts a changelog ChangeEntry to an EventEntry (kind=file).
func changeEventEntry(e ChangeEntry) EventEntry {
	entry := EventEntry{
		Kind:         EventKindFile,
		Op:           e.Op,
		Source:       e.Source,
		SessionID:    e.SessionID,
		User:         e.User,
		Label:        e.Label,
		AgentVersion: e.AgentVersion,
		Path:         e.Path,
		PrevPath:     e.PrevPath,
		SizeBytes:    e.SizeBytes,
		DeltaBytes:   e.DeltaBytes,
		ContentHash:  e.ContentHash,
		PrevHash:     e.PrevHash,
		Mode:         e.Mode,
		CheckpointID: e.CheckpointID,
	}
	if entry.SessionID != "" {
		entry.Actor = entry.SessionID
	} else {
		entry.Actor = "afs"
	}
	return entry
}

// changeEventEntries maps a slice of ChangeEntry values into EventEntry values.
func changeEventEntries(entries []ChangeEntry) []EventEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]EventEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, changeEventEntry(e))
	}
	return out
}
