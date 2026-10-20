// Quiet hours: releasing what was held. While a person's quiet window is on, a routine ask is recorded on its row with
// one exact hold note and posts nothing. One release job per quiet window, on the watchdog substrate (no timer of its
// own), checks the person's quiet state each time it's evaluated:
// - while quiet is on, it does nothing; so extending or replacing the window, or a job evaluated early, releases nothing;
// - once quiet is off ("I'm back", a clear, or the end of the window), each held ask that is still eligible is posted
//   individually through the ordinary gateway path, as itself: its full brief, its actual recipient and its reply
//   identity, with no mention and no second digest wait. An ask whose situation changed gets a visible disposition
//   instead;
// - the job stays until every held ask has its posted receipt or a disposition, so a restart or a failed post is
//   redriven, and the gateway's decision id makes a re-dispatch converge on one post;
// - a dispatched release can wait in the gateway's buffer and be replayed after a restart, so the gateway checks it
//   again at post time (the same `releaseVerdict`): quiet back on keeps it held for the job, and a changed row,
//   recipient or posting threshold gives the same disposition the job would. Nothing posts into changed circumstances.

import type { QueueRepository } from "../queue-repository.js";
import { isBlockerLive } from "../queue-repository.js";
import type { WatchdogJobsRepository } from "../watchdog-jobs-repository.js";
import { makeQueuePorts } from "../gateway/slack/queue-access.js";
import { OUTBOUND_OP, toPayload } from "../gateway/slack/outbound-driver.js";
import { loadConfig } from "../gateway/slack/config.js";
import { HumanQuietStore, quietHoldFor } from "../gateway/human-quiet-windows.js";
import type { QueueItem } from "../gateway/slack/queue-access.js";
import type { Policy, PolicyJob, PolicyEvaluation } from "./types.js";

export const QUIET_RELEASE_POLICY = "quiet-release";
const HOLD_PREFIX = "delivery-decision: quiet-hold";
const DISPATCHED_PREFIX = "quiet-release-dispatched";
const DISPOSITION_PREFIX = "quiet-release-disposition";
const RETURNED_PREFIX = "quiet-release-returned";

export interface QuietHold {
  windowId: string;
  occurrence: string;
  revision: number;
  notificationKey: string;
}

/** The one form of the hold note. Its prefix isn't the digest's, so the 4h/daily flush can never select it. */
export function formatQuietHoldNote(hold: QuietHold): string {
  return `${HOLD_PREFIX} window=${hold.windowId} occurrence=${hold.occurrence} revision=${hold.revision} notification_key=${hold.notificationKey}`;
}

export function parseQuietHoldNote(note: string): QuietHold | null {
  const m = /^delivery-decision: quiet-hold window=(\S+) occurrence=(\S+) revision=(\d+) notification_key=(\S+)$/.exec(note);
  return m ? { windowId: m[1]!, occurrence: m[2]!, revision: Number(m[3]), notificationKey: m[4]! } : null;
}

const hasToken = (note: string, prefix: string, key: string): boolean =>
  note.startsWith(`${prefix} `) && note.split(/\s+/).includes(`notification_key=${key}`);

/** Record a held ask on its row (once per episode) and make sure its window's release job is armed. */
export function holdForQuiet(input: {
  queueRepo: QueueRepository;
  jobsRepo: WatchdogJobsRepository;
  qitemId: string;
  entityId: string;
  hold: QuietHold;
}): void {
  const notes = input.queueRepo.listTransitions(input.qitemId).map((t) => t.transitionNote ?? "");
  if (!notes.some((n) => parseQuietHoldNote(n)?.notificationKey === input.hold.notificationKey)) {
    input.queueRepo.update({ qitemId: input.qitemId, actorSession: "daemon@kernel", transitionNote: formatQuietHoldNote(input.hold) });
  }
  armQuietRelease({ jobsRepo: input.jobsRepo, windowId: input.hold.windowId, entityId: input.entityId });
}

/** One active release job per quiet window; an existing one is reused. */
export function armQuietRelease(input: { jobsRepo: WatchdogJobsRepository; windowId: string; entityId: string }): { jobId: string } {
  const existing = input.jobsRepo.listActive().find(
    (j) => j.policy === QUIET_RELEASE_POLICY && j.specYaml.includes(`windowId: "${input.windowId}"`));
  if (existing) return { jobId: existing.jobId };
  const job = input.jobsRepo.register({
    policy: QUIET_RELEASE_POLICY,
    specYaml: ["context:", `  windowId: "${input.windowId}"`, `  entityId: "${input.entityId}"`].join("\n"),
    targetSession: `${input.entityId}@external`,
    intervalSeconds: 60,
    registeredBySession: "daemon@kernel",
  });
  return { jobId: job.jobId };
}

export type ReleaseDisposition = "row-not-active" | "superseded" | "recipient-changed" | "below-posting-threshold";
export type ReleaseVerdict =
  | { kind: "post"; alert: QueueItem }
  | { kind: "quiet"; windowId: string }
  | { kind: "disposition"; reason: ReleaseDisposition };

/** Whether a held ask should post now, decided from the current state: its recipient's quiet, the row, and the posting
 *  threshold. The release job asks before dispatching, and the gateway asks again at post time, so a release that was
 *  dispatched but not yet posted (buffered, then replayed after a restart) never posts into changed circumstances. */
export async function releaseVerdict(input: {
  queueRepo: QueueRepository;
  home: string;
  registry: RunQuietReleaseInput["registry"];
  entityId: string;
  qitemId: string;
  notificationKey: string;
  now: Date;
  alerts?: QueueItem[];
}): Promise<ReleaseVerdict> {
  const quiet = quietHoldFor(input.queueRepo.db, input.entityId, input.now);
  if (quiet) return { kind: "quiet", windowId: quiet.windowId };
  const alerts = input.alerts ?? await currentAlerts(input.queueRepo, input.home, input.registry);
  const alert = alerts.find((a) => (a.notificationKey ?? a.qitemId) === input.notificationKey);
  if (alert && (alert.destinationSession ?? "").split("@")[0] === input.entityId) return { kind: "post", alert };
  // Not a current alert for this person any more: say why, from the row as it is now.
  const row = input.queueRepo.getById(input.qitemId);
  const latest = row ? input.queueRepo.transitionLog.latestOwnerNotificationForQitem(input.qitemId) : null;
  const reason: ReleaseDisposition = !row || !isBlockerLive(row.state) ? "row-not-active"
    : latest && `${input.qitemId}:${latest.transitionId}` !== input.notificationKey ? "superseded"
      : alert ? "recipient-changed"
        : "below-posting-threshold";
  return { kind: "disposition", reason };
}

async function currentAlerts(queueRepo: QueueRepository, home: string, registry: RunQuietReleaseInput["registry"]): Promise<QueueItem[]> {
  const reg = registry.loadHumanRegistry(home);
  if (!reg.ok) return [];
  return makeQueuePorts(queueRepo, { loadHumanRegistry: () => reg } as never).listHumanAlerts({ minimumLevel: loadConfig(home).minimumLevelThatPosts });
}

/** The gateway's half: re-check a release at post time. True: post it. False: it was not posted, and the row says why. */
export async function confirmReleaseAtPost(input: {
  queueRepo: QueueRepository;
  jobsRepo: WatchdogJobsRepository;
  home: string;
  registry: RunQuietReleaseInput["registry"];
  entityId: string;
  qitemId: string;
  notificationKey: string;
  now?: Date;
}): Promise<boolean> {
  const verdict = await releaseVerdict({ ...input, now: input.now ?? new Date() });
  if (verdict.kind === "post") return true;
  const key = input.notificationKey;
  if (verdict.kind === "quiet") {
    // Quiet came back on before this release posted: it stays held, and its window's job releases it next time.
    input.queueRepo.update({ qitemId: input.qitemId, actorSession: "daemon@kernel",
      transitionNote: `${RETURNED_PREFIX} window=${verdict.windowId} notification_key=${key} reason=quiet-on` });
    armQuietRelease({ jobsRepo: input.jobsRepo, windowId: verdict.windowId, entityId: input.entityId });
    return false;
  }
  const notes = input.queueRepo.listTransitions(input.qitemId).map((t) => t.transitionNote ?? "");
  if (!notes.some((n) => hasToken(n, DISPOSITION_PREFIX, key))) {
    const windowId = notes.map(parseQuietHoldNote).find((h) => h?.notificationKey === key)?.windowId ?? "unknown";
    input.queueRepo.update({ qitemId: input.qitemId, actorSession: "daemon@kernel",
      transitionNote: `${DISPOSITION_PREFIX} window=${windowId} notification_key=${key} reason=${verdict.reason}` });
  }
  return false;
}

export interface RunQuietReleaseInput {
  queueRepo: QueueRepository;
  jobsRepo: WatchdogJobsRepository;
  home: string;
  registry: { loadHumanRegistry: (home: string) => { ok: boolean; entities?: Array<{ entityId: string }> } };
  dispatch: (op: string, entityBindingRef: string, payload: unknown, opts?: { decisionId?: string }) => { ok: boolean; error?: string };
  jobId: string;
  now?: Date;
}

/** One evaluation of a window's release job. `quiet`: nothing done. `pending`: posts dispatched, receipts awaited.
 *  `released`: every held ask has its receipt or a disposition, and the job is retired. */
export async function runQuietRelease(input: RunQuietReleaseInput): Promise<{ state: "quiet" | "pending" | "released"; released: number; dispositions: number }> {
  const job = input.jobsRepo.getById(input.jobId);
  const grab = (k: string) => job?.specYaml.match(new RegExp(`${k}:\\s*"([^"\\n]+)"`))?.[1];
  const windowId = grab("windowId");
  const entityId = grab("entityId");
  if (!job || job.state !== "active" || !windowId || !entityId) return { state: "released", released: 0, dispositions: 0 };
  const now = input.now ?? new Date();
  if (new HumanQuietStore(input.queueRepo.db).quietStateFor(entityId, now).active) return { state: "quiet", released: 0, dispositions: 0 };

  // Every ask held under this window, whichever occurrence or revision held it.
  const held = new Map<string, string>(); // notification key → qitem id
  const rows = input.queueRepo.db.prepare(
    "SELECT qitem_id AS qitemId, transition_note AS note FROM queue_transitions WHERE transition_note LIKE ? ORDER BY transition_id",
  ).all(`${HOLD_PREFIX} window=${windowId} %`) as Array<{ qitemId: string; note: string }>;
  for (const r of rows) {
    const hold = parseQuietHoldNote(r.note);
    if (hold && hold.windowId === windowId) held.set(hold.notificationKey, r.qitemId);
  }

  const alerts = await currentAlerts(input.queueRepo, input.home, input.registry);
  let released = 0;
  let dispositions = 0;
  let open = 0;
  for (const [key, qitemId] of held) {
    const notes = input.queueRepo.listTransitions(qitemId).map((t) => t.transitionNote ?? "");
    if (notes.some((n) => hasToken(n, "slack-owner-notification-posted", key) || hasToken(n, DISPOSITION_PREFIX, key))) continue;
    const verdict = await releaseVerdict({ queueRepo: input.queueRepo, home: input.home, registry: input.registry, entityId, qitemId,
      notificationKey: key, now, alerts });
    if (verdict.kind === "quiet") { open++; continue; }
    if (verdict.kind === "post") {
      open++;
      const res = input.dispatch(OUTBOUND_OP, verdict.alert.destinationSession!, { ...toPayload(verdict.alert), quietRelease: true }, { decisionId: `quiet-release:${key}` });
      if (res.ok && !notes.some((n) => hasToken(n, DISPATCHED_PREFIX, key))) {
        released++;
        input.queueRepo.update({ qitemId, actorSession: "daemon@kernel", transitionNote: `${DISPATCHED_PREFIX} window=${windowId} notification_key=${key}` });
      }
      continue;
    }
    input.queueRepo.update({ qitemId, actorSession: "daemon@kernel",
      transitionNote: `${DISPOSITION_PREFIX} window=${windowId} notification_key=${key} reason=${verdict.reason}` });
    dispositions++;
  }
  if (open === 0) {
    input.jobsRepo.markTerminal(input.jobId, "quiet release complete: every held ask posted or given a disposition");
    return { state: "released", released, dispositions };
  }
  return { state: "pending", released, dispositions };
}

/** Watchdog-engine policy wrapper: the scheduler's cadence drives evaluation; the current quiet state owns the timing. */
export function makeQuietReleasePolicy(deps: Omit<RunQuietReleaseInput, "jobId" | "now">): Policy {
  return {
    name: QUIET_RELEASE_POLICY,
    async evaluate(job: PolicyJob): Promise<PolicyEvaluation> {
      const r = await runQuietRelease({ ...deps, jobId: job.jobId });
      if (r.state === "released") return { action: "terminal", reason: "quiet release complete" };
      return { action: "skip", reason: r.state === "quiet" ? "quiet is still on" : `released ${r.released}; awaiting receipts` };
    },
  } as Policy;
}
