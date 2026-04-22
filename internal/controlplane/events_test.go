package controlplane

import (
	"context"
	"testing"
)

func TestAuditDualWritesToEventsStream(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	// Audit needs a workspace meta to resolve the storage ID. Plant one.
	meta := WorkspaceMeta{ID: "ws1", Name: "ws1"}
	if err := store.PutWorkspaceMeta(ctx, meta); err != nil {
		t.Fatalf("put workspace meta: %v", err)
	}

	if err := store.Audit(ctx, "ws1", "session_start", map[string]any{
		"session_id":  "sess-1",
		"client_kind": "agent",
		"hostname":    "host-a",
	}); err != nil {
		t.Fatalf("audit: %v", err)
	}

	resp, err := store.ListEvents(ctx, "ws1", EventsListRequest{Limit: 10})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Fatalf("events count = %d, want 1", len(resp.Entries))
	}
	row := resp.Entries[0]
	if row.Kind != EventKindSession {
		t.Errorf("kind = %q, want %q", row.Kind, EventKindSession)
	}
	if row.Op != "start" {
		t.Errorf("op = %q, want start", row.Op)
	}
	if row.SessionID != "sess-1" {
		t.Errorf("session_id = %q, want sess-1", row.SessionID)
	}
	if row.Hostname != "host-a" {
		t.Errorf("hostname = %q, want host-a", row.Hostname)
	}
	if row.Extras["client_kind"] != "agent" {
		t.Errorf("extras.client_kind = %q, want agent", row.Extras["client_kind"])
	}
}

func TestEnqueueChangeEntriesDualWritesFileEvents(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	entries := []ChangeEntry{
		{
			SessionID:    "sess-x",
			Op:           ChangeOpPut,
			Path:         "/a.txt",
			SizeBytes:    10,
			DeltaBytes:   10,
			ContentHash:  "blob-a",
			Source:       ChangeSourceCheckpoint,
			CheckpointID: "cp-1",
		},
	}
	pipe := store.rdb.Pipeline()
	enqueueChangeEntries(ctx, pipe, "ws1", entries)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("exec: %v", err)
	}

	resp, err := store.ListEvents(ctx, "ws1", EventsListRequest{Limit: 10})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Fatalf("events count = %d, want 1", len(resp.Entries))
	}
	row := resp.Entries[0]
	if row.Kind != EventKindFile {
		t.Errorf("kind = %q, want file", row.Kind)
	}
	if row.Op != ChangeOpPut {
		t.Errorf("op = %q, want put", row.Op)
	}
	if row.Path != "/a.txt" {
		t.Errorf("path = %q, want /a.txt", row.Path)
	}
	if row.CheckpointID != "cp-1" {
		t.Errorf("checkpoint_id = %q, want cp-1", row.CheckpointID)
	}
	if row.ContentHash != "blob-a" {
		t.Errorf("content_hash = %q, want blob-a", row.ContentHash)
	}
	if row.Source != ChangeSourceCheckpoint {
		t.Errorf("source = %q, want %q", row.Source, ChangeSourceCheckpoint)
	}
	if row.Actor != "sess-x" {
		t.Errorf("actor = %q, want sess-x", row.Actor)
	}
}

func TestListEventsFiltersByKindAndSession(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	writeEvents(ctx, store.rdb, "ws1", []EventEntry{
		{Kind: EventKindSession, Op: "start", SessionID: "sess-a"},
		{Kind: EventKindFile, Op: ChangeOpPut, Path: "/one", SessionID: "sess-a"},
		{Kind: EventKindFile, Op: ChangeOpPut, Path: "/two", SessionID: "sess-b"},
		{Kind: EventKindCheckpoint, Op: "save", CheckpointID: "cp-1"},
	})

	all, err := store.ListEvents(ctx, "ws1", EventsListRequest{Limit: 100})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all.Entries) != 4 {
		t.Fatalf("list-all count = %d, want 4", len(all.Entries))
	}

	fileOnly, err := store.ListEvents(ctx, "ws1", EventsListRequest{
		Kinds: []string{EventKindFile},
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("list file: %v", err)
	}
	if len(fileOnly.Entries) != 2 {
		t.Fatalf("file-only count = %d, want 2", len(fileOnly.Entries))
	}

	sessA, err := store.ListEvents(ctx, "ws1", EventsListRequest{
		SessionID: "sess-a",
		Limit:     100,
	})
	if err != nil {
		t.Fatalf("list sess-a: %v", err)
	}
	if len(sessA.Entries) != 2 {
		t.Fatalf("sess-a count = %d, want 2", len(sessA.Entries))
	}
	for _, row := range sessA.Entries {
		if row.SessionID != "sess-a" {
			t.Errorf("leaked session %q", row.SessionID)
		}
	}

	pathFiltered, err := store.ListEvents(ctx, "ws1", EventsListRequest{
		Kinds: []string{EventKindFile},
		Path:  "/one",
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("list path: %v", err)
	}
	if len(pathFiltered.Entries) != 1 {
		t.Fatalf("path count = %d, want 1", len(pathFiltered.Entries))
	}
	if pathFiltered.Entries[0].Path != "/one" {
		t.Errorf("path = %q, want /one", pathFiltered.Entries[0].Path)
	}
}

func TestAuditEventEntryMapping(t *testing.T) {
	cases := []struct {
		op      string
		extra   map[string]any
		wantKind string
		wantOp   string
	}{
		{"workspace_create", map[string]any{"checkpoint": "cp-init"}, EventKindWorkspace, "create"},
		{"workspace_fork", map[string]any{"source_workspace": "other"}, EventKindWorkspace, "fork"},
		{"import", map[string]any{"source": "dir"}, EventKindWorkspace, "import"},
		{"session_close", map[string]any{"session_id": "s1"}, EventKindSession, "close"},
		{"save", map[string]any{"savepoint": "cp-42"}, EventKindCheckpoint, "save"},
		{"checkpoint_restore", map[string]any{"checkpoint": "cp-7"}, EventKindCheckpoint, "restore"},
		{"run_start", map[string]any{"argv": "go test"}, EventKindProcess, "start"},
	}
	for _, tc := range cases {
		entry, ok := auditEventEntry(tc.op, tc.extra)
		if !ok {
			t.Errorf("op %q: mapping missing", tc.op)
			continue
		}
		if entry.Kind != tc.wantKind {
			t.Errorf("op %q: kind = %q, want %q", tc.op, entry.Kind, tc.wantKind)
		}
		if entry.Op != tc.wantOp {
			t.Errorf("op %q: op = %q, want %q", tc.op, entry.Op, tc.wantOp)
		}
	}
	if _, ok := auditEventEntry("unknown_op", nil); ok {
		t.Error("unknown op should not map")
	}
}
