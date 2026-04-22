package main

import (
	"strings"
	"testing"
)

func TestParseEventsArgs(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    eventsFlags
		wantErr bool
	}{
		{
			name: "defaults",
			args: nil,
			want: eventsFlags{limit: 100},
		},
		{
			name: "workspace and session flags",
			args: []string{"--workspace", "repo", "--session", "sess-1"},
			want: eventsFlags{limit: 100, workspace: "repo", sessionID: "sess-1"},
		},
		{
			name: "kind list splits CSV",
			args: []string{"--kind", "file,checkpoint", "--kind=session"},
			want: eventsFlags{limit: 100, kinds: []string{"file", "checkpoint", "session"}},
		},
		{
			name: "limit validation",
			args: []string{"--limit", "0"},
			wantErr: true,
		},
		{
			name: "path filter",
			args: []string{"--path", "/src/main.go"},
			want: eventsFlags{limit: 100, path: "/src/main.go"},
		},
		{
			name: "rejects unknown flag",
			args: []string{"--bogus"},
			wantErr: true,
		},
		{
			name: "rejects positional",
			args: []string{"positional"},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseEventsArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.workspace != tc.want.workspace {
				t.Errorf("workspace = %q, want %q", got.workspace, tc.want.workspace)
			}
			if got.sessionID != tc.want.sessionID {
				t.Errorf("sessionID = %q, want %q", got.sessionID, tc.want.sessionID)
			}
			if got.path != tc.want.path {
				t.Errorf("path = %q, want %q", got.path, tc.want.path)
			}
			if got.limit != tc.want.limit {
				t.Errorf("limit = %d, want %d", got.limit, tc.want.limit)
			}
			if strings.Join(got.kinds, ",") != strings.Join(tc.want.kinds, ",") {
				t.Errorf("kinds = %v, want %v", got.kinds, tc.want.kinds)
			}
		})
	}
}

func TestResolveSinceCursor(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr bool
		check   func(t *testing.T, got string)
	}{
		{name: "empty passes through", raw: "", check: func(t *testing.T, got string) {
			if got != "" {
				t.Errorf("empty should stay empty, got %q", got)
			}
		}},
		{name: "stream id round-trips", raw: "1700000000-0", check: func(t *testing.T, got string) {
			if got != "1700000000-0" {
				t.Errorf("stream id should round-trip, got %q", got)
			}
		}},
		{name: "duration", raw: "30m", check: func(t *testing.T, got string) {
			if !strings.HasSuffix(got, "-0") || len(got) <= 2 {
				t.Errorf("duration should yield ms-0 cursor, got %q", got)
			}
		}},
		{name: "bad value", raw: "nope", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveSinceCursor(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
}
