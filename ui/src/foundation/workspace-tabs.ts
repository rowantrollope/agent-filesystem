import { z } from "zod";

export const studioTabValues = ["browse", "checkpoints", "history", "settings"] as const;

export type StudioTab = (typeof studioTabValues)[number];

// Accept legacy tab values ("activity", "changes") and redirect them to the
// unified "history" tab so existing bookmarks keep working after the merge.
const legacyHistoryAliases = ["activity", "changes"] as const;

export const studioTabSchema = z
  .enum([...studioTabValues, ...legacyHistoryAliases])
  .transform((value) =>
    (legacyHistoryAliases as readonly string[]).includes(value) ? ("history" as StudioTab) : (value as StudioTab),
  );
