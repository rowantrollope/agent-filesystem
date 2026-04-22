import { useMemo, useState } from "react";
import styled from "styled-components";
import {
  SectionCard,
  SectionGrid,
  SectionHeader,
  SectionTitle,
} from "../../components/afs-kit";
import { useEvents } from "../../foundation/hooks/use-afs";
import type { AFSEventEntry, AFSEventKind } from "../../foundation/types/afs";

type Props = {
  databaseId?: string;
  workspaceId: string;
};

type KindPill = {
  kind: AFSEventKind;
  label: string;
  defaultOn: boolean;
};

// Lifecycle pills are on by default. File-ops is off by default so agent
// sessions don't swamp the feed — matches the bet in the unresolved-questions
// section of plans/event-history-merge.md.
const KIND_PILLS: KindPill[] = [
  { kind: "workspace", label: "Workspace", defaultOn: true },
  { kind: "session", label: "Session", defaultOn: true },
  { kind: "checkpoint", label: "Checkpoint", defaultOn: true },
  { kind: "process", label: "Process", defaultOn: true },
  { kind: "file", label: "File changes", defaultOn: false },
];

function formatBytes(delta: number | undefined): string {
  if (!delta) return "";
  const abs = Math.abs(delta);
  const sign = delta > 0 ? "+" : "−";
  if (abs < 1024) return `${sign}${abs} B`;
  if (abs < 1024 * 1024) return `${sign}${(abs / 1024).toFixed(1)} KB`;
  if (abs < 1024 * 1024 * 1024) return `${sign}${(abs / (1024 * 1024)).toFixed(1)} MB`;
  return `${sign}${(abs / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function kindColor(kind: AFSEventKind): string {
  switch (kind) {
    case "file":
      return "#0ea5e9";
    case "checkpoint":
      return "#8b5cf6";
    case "session":
      return "#16a34a";
    case "process":
      return "#f59e0b";
    default:
      return "#64748b";
  }
}

function formatTimestamp(raw?: string): string {
  if (!raw) return "";
  const ts = new Date(raw);
  if (Number.isNaN(ts.getTime())) return raw;
  return ts.toLocaleString();
}

function summarizeEvent(event: AFSEventEntry): string {
  switch (event.kind) {
    case "file": {
      const path = event.prevPath ? `${event.prevPath} → ${event.path}` : event.path ?? "";
      const delta = formatBytes(event.deltaBytes);
      return delta ? `${path}  ${delta}` : path;
    }
    case "checkpoint":
      return event.checkpointId ?? "";
    case "session": {
      const session = event.sessionId ? event.sessionId.slice(0, 12) : "";
      const host = event.hostname ?? "";
      if (session && host) return `${session} · ${host}`;
      return session || host;
    }
    case "workspace": {
      const extras = event.extras ?? {};
      if (extras.source_workspace) {
        return `from ${extras.source_workspace}`;
      }
      if (extras.source) {
        return `source: ${extras.source}`;
      }
      return "";
    }
    case "process": {
      const extras = event.extras ?? {};
      const argv = extras.argv ?? "";
      const exit = extras.exit_code ? ` (exit=${extras.exit_code})` : "";
      return `${argv}${exit}`;
    }
    default:
      return "";
  }
}

export function HistoryTab({ databaseId, workspaceId }: Props) {
  const [kindFilters, setKindFilters] = useState<Record<AFSEventKind, boolean>>(
    () =>
      KIND_PILLS.reduce((acc, pill) => {
        acc[pill.kind] = pill.defaultOn;
        return acc;
      }, {} as Record<AFSEventKind, boolean>),
  );

  const activeKinds = useMemo(
    () => KIND_PILLS.filter((p) => kindFilters[p.kind]).map((p) => p.kind),
    [kindFilters],
  );

  const allOn = activeKinds.length === KIND_PILLS.length;
  const anyOn = activeKinds.length > 0;

  const query = useEvents(
    {
      databaseId,
      workspaceId,
      // Only pass `kinds` when a strict subset is selected. If all pills are
      // on, omit the filter so we read everything; if nothing is selected,
      // still send an impossible filter so the response is empty.
      kinds: allOn ? undefined : activeKinds,
      limit: 200,
      direction: "desc",
    },
    anyOn,
  );

  const entries = query.data?.entries ?? [];

  return (
    <SectionGrid>
      <SectionCard $span={12}>
        <SectionHeader>
          <SectionTitle title="History" />
          <HeaderSummary>
            {query.isLoading
              ? "Loading…"
              : entries.length > 0
                ? `${entries.length} events`
                : anyOn
                  ? "No events yet"
                  : "Select a kind to show events"}
          </HeaderSummary>
        </SectionHeader>
        <PillRow>
          {KIND_PILLS.map((pill) => {
            const active = kindFilters[pill.kind];
            return (
              <Pill
                key={pill.kind}
                type="button"
                $active={active}
                $color={kindColor(pill.kind)}
                onClick={() =>
                  setKindFilters((prev) => ({ ...prev, [pill.kind]: !prev[pill.kind] }))
                }
              >
                {pill.label}
              </Pill>
            );
          })}
        </PillRow>
        {query.isError ? (
          <ErrorState>
            {query.error instanceof Error
              ? query.error.message
              : "Unable to load history. Please retry."}
          </ErrorState>
        ) : null}
        <EventList>
          {entries.map((event) => (
            <EventRow key={event.id}>
              <When>{formatTimestamp(event.occurredAt)}</When>
              <KindBadge $color={kindColor(event.kind)}>{event.kind}</KindBadge>
              <Op>{event.op}</Op>
              <Detail>{summarizeEvent(event)}</Detail>
            </EventRow>
          ))}
          {entries.length === 0 && !query.isLoading && !query.isError && anyOn ? (
            <EmptyRow>No events match the current filter.</EmptyRow>
          ) : null}
        </EventList>
      </SectionCard>
    </SectionGrid>
  );
}

const HeaderSummary = styled.span`
  color: var(--afs-muted);
  font-size: 13px;
  white-space: nowrap;
`;

const PillRow = styled.div`
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  padding: 12px 0;
`;

const Pill = styled.button<{ $active: boolean; $color: string }>`
  border: 1px solid ${(p) => (p.$active ? p.$color : "var(--afs-border)")};
  background: ${(p) => (p.$active ? `${p.$color}22` : "transparent")};
  color: ${(p) => (p.$active ? p.$color : "var(--afs-muted)")};
  padding: 4px 12px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;

  &:hover {
    border-color: ${(p) => p.$color};
    color: ${(p) => p.$color};
  }
`;

const EventList = styled.div`
  display: flex;
  flex-direction: column;
  gap: 2px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 12px;
`;

const EventRow = styled.div`
  display: grid;
  grid-template-columns: 180px 110px 80px 1fr;
  gap: 8px;
  padding: 6px 8px;
  border-bottom: 1px solid var(--afs-border);

  &:last-child {
    border-bottom: none;
  }
`;

const When = styled.span`
  color: var(--afs-muted);
`;

const KindBadge = styled.span<{ $color: string }>`
  color: ${(p) => p.$color};
  font-weight: 700;
  text-transform: uppercase;
`;

const Op = styled.span`
  color: var(--afs-ink);
  font-weight: 600;
`;

const Detail = styled.span`
  color: var(--afs-ink);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

const EmptyRow = styled.div`
  padding: 24px;
  text-align: center;
  color: var(--afs-muted);
`;

const ErrorState = styled.div`
  padding: 12px;
  color: #dc2626;
  font-size: 13px;
`;
