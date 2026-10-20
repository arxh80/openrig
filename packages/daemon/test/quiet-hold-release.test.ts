// Quiet hours hold a person's routine asks and release them when quiet ends. The decision (one new outcome, `hold`),
// the exact hold note, the digest flush's re-check for asks decided before quiet began, and the release job that posts
// each still-eligible ask individually through the ordinary Slack path. Real SQLite (all migrations), the real queue,
// the real gateway wire over a recording fetch, and the real watchdog jobs table. No Slack. Native: NOT RUN.
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository, type QueueItem } from "../src/domain/queue-repository.js";
import { makeQueuePorts } from "../src/domain/gateway/slack/queue-access.js";
import { DEFAULT_CONFIG, saveConfig } from "../src/domain/gateway/slack/config.js";
import { buildSlackGatewayWire } from "../src/domain/gateway/slack/slack-subsystem.js";
import { OUTBOUND_OP, toPayload } from "../src/domain/gateway/slack/outbound-driver.js";
import { DispatchBuffer } from "../src/domain/gateway/dispatch-buffer.js";
import { resolveSlackHandle } from "../src/domain/gateway/human-registry.js";
import { WatchdogJobsRepository } from "../src/domain/watchdog-jobs-repository.js";
import { HumanQuietStore } from "../src/domain/gateway/human-quiet-windows.js";
import { decideDelivery, type DeliveryDecisionInput } from "../src/domain/gateway/delivery-rules-engine.js";
import { runDeliveryDigestFlush } from "../src/domain/policies/delivery-digest-flush.js";
import { formatQuietHoldNote, parseQuietHoldNote, QUIET_RELEASE_POLICY, runQuietRelease } from "../src/domain/policies/quiet-release.js";
import { makeOperatorDeliveryEngine } from "../src/domain/gateway/operator-delivery-engine.js";

const HUMAN = "human-founder";
const settle = () => new Promise((resolve) => setTimeout(resolve, 60));

function registryWith(prefs: Record<string, unknown>) {
  return {
    ok: true as const,
    entities: [{
      entityId: HUMAN, class: "human" as const, displayName: "Founder", address: `${HUMAN}@external`,
      connectorBindings: [{ kind: "slack" as const, connectorRef: "primary", secretsRef: "env:SLACK_BOT_TOKEN", role: "primary" as const, handle: "UFOUNDER" }],
      prefs,
    }],
  };
}

// ─── The decision ──────────────────────────────────────────────────────────────────────────────────────────────────

describe("the hold decision", () => {
  const quiet = { windowId: "w-1", occurrence: "2026-10-20", revision: 3 };
  const cells: DeliveryDecisionInput[] = [];
  for (const deliveryClass of ["A", "B", "C", "D"] as const) {
    for (const availability of ["available", "focus", "away", "off"] as const) {
      for (const escalation of [false, true]) {
        for (const level of ["RECORD", "NOTICE", "ALERT"] as const) {
          cells.push({ level, escalation, human: { entityId: HUMAN, deliveryClass, availability },
            dials: { minimumLevelThatPosts: "NOTICE", minimumLevelThatInterrupts: "ALERT" } });
        }
      }
    }
  }

  it("outside quiet, every one of the 96 cells decides exactly as today", () => {
    for (const cell of cells) expect(decideDelivery({ ...cell, quiet: null })).toEqual(decideDelivery(cell));
  });

  it("during quiet, every routine ask that would post is held, class A included, with no mention", () => {
    for (const cell of cells.filter((c) => !c.escalation)) {
      const today = decideDelivery(cell);
      const held = decideDelivery({ ...cell, quiet });
      if (today.outcome === "log") expect(held, JSON.stringify(cell)).toEqual(today); // the posting dial still logs only
      else expect(held, JSON.stringify(cell)).toEqual({ outcome: "hold", mention: false, quietHold: quiet });
    }
  });

  it("during quiet, an urgent (escalation) ask decides exactly as outside it: at once, away's 30 minutes, off's no mention", () => {
    for (const cell of cells.filter((c) => c.escalation)) expect(decideDelivery({ ...cell, quiet })).toEqual(decideDelivery(cell));
    const away = decideDelivery({ ...cells.find((c) => c.escalation && c.human.availability === "away" && c.human.deliveryClass === "B" && c.level === "ALERT")!, quiet });
    expect(away).toMatchObject({ outcome: "interrupt", deferMinutes: 30 });
    const off = decideDelivery({ ...cells.find((c) => c.escalation && c.human.availability === "off" && c.level === "ALERT")!, quiet });
    expect(off).toMatchObject({ outcome: "notify", mention: false });
  });
});

describe("the hold note", () => {
  it("has one exact, parseable form that the 4h/daily flush's digest selection can't match", () => {
    const note = formatQuietHoldNote({ windowId: "w-1", occurrence: "2026-10-20", revision: 3, notificationKey: "q-1:7" });
    expect(note).toBe("delivery-decision: quiet-hold window=w-1 occurrence=2026-10-20 revision=3 notification_key=q-1:7");
    expect(note.startsWith("delivery-decision: digest")).toBe(false);
    expect(parseQuietHoldNote(note)).toEqual({ windowId: "w-1", occurrence: "2026-10-20", revision: 3, notificationKey: "q-1:7" });
    expect(parseQuietHoldNote("delivery-decision: digest window=4h notification_key=q-1:7")).toBeNull();
    expect(parseQuietHoldNote(`${note} extra=1`)).toBeNull();
  });
});

// ─── Through the real gateway ───────────────────────────────────────────────────────────────────────────────────────

describe("holding and releasing through the gateway", () => {
  let db: Database.Database;
  let repo: QueueRepository;
  let home: string;
  let quiet: HumanQuietStore;
  let prefs: Record<string, unknown>;
  const posts: Array<Record<string, unknown>> = [];

  beforeEach(() => {
    db = createDb();
    migrate(db, ALL_MIGRATIONS);
    prefs = { deliveryClass: "B", availability: "available" };
    repo = new QueueRepository(db, new EventBus(db), { validateRig: () => true, loadHumanRegistry: () => registryWith(prefs) });
    home = mkdtempSync(join(tmpdir(), "quiet-hold-"));
    quiet = new HumanQuietStore(db);
    posts.length = 0;
  });
  afterEach(() => { db.close(); rmSync(home, { recursive: true, force: true }); });

  let n = 0;
  async function park(opts: { tags?: string[] } = {}): Promise<QueueItem> {
    const row = await repo.create({ sourceSession: "dev-qa@r", destinationSession: "orch-lead@r", body: `decision body ${++n}`,
      ...(opts.tags ? { tags: opts.tags } : {}), nudge: false });
    repo.update({ qitemId: row.qitemId, actorSession: "orch-lead@r", state: "blocked", blockedOn: `${HUMAN}@kernel`,
      summary: `Choose ${n}`, evidenceRef: `/proof/${n}.md`, transitionNote: "parked" });
    return row;
  }
  function wire() {
    const secrets = join(home, "slack.env");
    writeFileSync(secrets, "SLACK_BOT_TOKEN=xoxb-EXAMPLE-fake\n", { mode: 0o600 });
    saveConfig({ ...DEFAULT_CONFIG, enabled: true, channel: "C-OWNER", secretsEnvFile: secrets }, home);
    return buildSlackGatewayWire({
      home, queueRepo: repo, registry: { loadHumanRegistry: () => registryWith(prefs), resolveSlackHandle }, outboundIntervalMs: 60_000,
      fetchImpl: async (url, init) => {
        if (String(url).includes("chat.postMessage")) {
          posts.push(JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>);
          return new Response(JSON.stringify({ ok: true, ts: `1724.9${posts.length}` }), { status: 200, headers: { "content-type": "application/json" } });
        }
        return new Response(JSON.stringify({ ok: true, messages: [] }), { status: 200, headers: { "content-type": "application/json" } });
      },
    });
  }
  const settle = () => new Promise((resolve) => setTimeout(resolve, 40));
  async function alertFor(row: QueueItem) {
    const ports = makeQueuePorts(repo, { loadHumanRegistry: () => registryWith(prefs) } as never);
    return (await ports.listHumanAlerts({ minimumLevel: "NOTICE" })).find((a) => a.qitemId === row.qitemId)!;
  }
  const notes = (row: QueueItem) => repo.listTransitions(row.qitemId).map((t) => t.transitionNote ?? "");
  const releaseJobs = () => new WatchdogJobsRepository(db).listActive().filter((j) => j.policy === QUIET_RELEASE_POLICY);
  async function release(w: ReturnType<typeof wire>, now = new Date()) {
    const [job] = releaseJobs();
    if (!job) return null;
    return runQuietRelease({ queueRepo: repo, jobsRepo: new WatchdogJobsRepository(db), home, jobId: job.jobId, now,
      registry: { loadHumanRegistry: () => registryWith(prefs) },
      dispatch: (op, ref, payload, opts) => w.dispatcher.dispatch(op, ref, payload, opts as never) });
  }
  function quietNow() {
    const now = new Date();
    quiet.setUntil(HUMAN, { until: new Date(now.getTime() + 3_600_000), timezone: "UTC", setBy: "test", now });
    return quiet.quietStateFor(HUMAN, now) as { windowId: string; revision: number };
  }

  it("during quiet a routine ask posts nothing, carries the exact hold note, and arms one release job", async () => {
    const state = quietNow();
    const row = await park();
    const alert = await alertFor(row);
    const w = wire();
    try {
      w.startServices?.();
      w.dispatcher.dispatch(OUTBOUND_OP, alert.destinationSession!, alert);
      await settle();
      expect(posts).toHaveLength(0);
      const key = alert.notificationKey ?? alert.qitemId;
      expect(notes(row)).toContain(`delivery-decision: quiet-hold window=${state.windowId} occurrence=until revision=${state.revision} notification_key=${key}`);
      expect(releaseJobs()).toHaveLength(1);
    } finally { w.stop(); }
  });

  it("during quiet an urgent ask posts at once", async () => {
    quietNow();
    const row = await park({ tags: ["escalation"] });
    const w = wire();
    try {
      w.startServices?.();
      w.dispatcher.dispatch(OUTBOUND_OP, `${HUMAN}@external`, await alertFor(row));
      await settle();
      expect(posts).toHaveLength(1);
      expect(notes(row).some((n) => n.startsWith("delivery-decision: quiet-hold"))).toBe(false);
    } finally { w.stop(); }
  });

  it("nothing is released while quiet is on, even when the window is extended; once it ends each held ask posts individually, once, with no mention", async () => {
    quietNow();
    const rows = [await park(), await park()];
    const w = wire();
    try {
      w.startServices?.();
      for (const row of rows) w.dispatcher.dispatch(OUTBOUND_OP, `${HUMAN}@external`, await alertFor(row));
      await settle();
      expect(await release(w)).toMatchObject({ state: "quiet", released: 0 });
      quiet.setUntil(HUMAN, { until: new Date(Date.now() + 7_200_000), timezone: "UTC", setBy: "test", now: new Date() }); // extend
      expect(await release(w)).toMatchObject({ state: "quiet", released: 0 });
      await settle();
      expect(posts).toHaveLength(0);

      quiet.end(HUMAN, { endedBy: "test", now: new Date() }); // "I'm back"
      expect(await release(w)).toMatchObject({ state: "pending", released: 2 });
      await settle();
      expect(posts).toHaveLength(2);
      for (const row of rows) expect(JSON.stringify(posts)).toContain(String(row.body));
      expect(JSON.stringify(posts)).not.toContain("<@UFOUNDER>");
      for (const row of rows) expect(notes(row).filter((n) => n.startsWith("slack-owner-notification-posted "))).toHaveLength(1);
      // The next pass sees every receipt and retires the job; nothing posts twice.
      expect(await release(w)).toMatchObject({ state: "released", released: 0 });
      await settle();
      expect(posts).toHaveLength(2);
      expect(releaseJobs()).toHaveLength(0);
    } finally { w.stop(); }
  });

  it("an ask whose row closed during quiet gets a visible disposition, not a post", async () => {
    quietNow();
    const [kept, closed] = [await park(), await park()];
    const w = wire();
    try {
      w.startServices?.();
      for (const row of [kept, closed]) w.dispatcher.dispatch(OUTBOUND_OP, `${HUMAN}@external`, await alertFor(row));
      await settle();
      repo.update({ qitemId: closed.qitemId, actorSession: "orch-lead@r", state: "done", closureReason: "no-follow-on", transitionNote: "answered elsewhere" });
      quiet.clear(HUMAN, { clearedBy: "test", now: new Date() });
      expect(await release(w)).toMatchObject({ released: 1, dispositions: 1 });
      await settle();
      expect(posts).toHaveLength(1);
      expect(JSON.stringify(posts)).toContain(String(kept.body));
      expect(notes(closed).some((n) => n.startsWith("quiet-release-disposition ") && n.includes("reason=row-not-active"))).toBe(true);
    } finally { w.stop(); }
  });

  it("after a restart (new repositories over the same database) a release that is due catches up", async () => {
    quietNow();
    const row = await park();
    const w = wire();
    try {
      w.startServices?.();
      w.dispatcher.dispatch(OUTBOUND_OP, `${HUMAN}@external`, await alertFor(row));
      await settle();
      quiet.end(HUMAN, { endedBy: "test", now: new Date() });
      repo = new QueueRepository(db, new EventBus(db), { validateRig: () => true, loadHumanRegistry: () => registryWith(prefs) });
      expect(await release(w)).toMatchObject({ released: 1 });
      await settle();
      expect(posts).toHaveLength(1);
    } finally { w.stop(); }
  });

  it("an ask put in the digest before quiet began is held at flush time instead of posted, and released individually after", async () => {
    prefs = { deliveryClass: "C", availability: "available" };
    const row = await park();
    const alert = await alertFor(row);
    const key = alert.notificationKey ?? alert.qitemId;
    repo.update({ qitemId: row.qitemId, actorSession: "daemon@kernel", transitionNote: `delivery-decision: digest window=4h notification_key=${key}` });
    const state = quietNow();
    const w = wire();
    try {
      w.startServices?.();
      const flushed = await runDeliveryDigestFlush({ queueRepo: repo, registry: { loadHumanRegistry: () => registryWith(prefs) }, home, window: "4h",
        dispatch: (op, ref, payload, opts) => w.dispatcher.dispatch(op, ref, payload, opts as never) });
      await settle();
      expect(flushed).toMatchObject({ dispatched: 0 });
      expect(posts).toHaveLength(0);
      expect(notes(row)).toContain(`delivery-decision: quiet-hold window=${state.windowId} occurrence=until revision=${state.revision} notification_key=${key}`);
      quiet.end(HUMAN, { endedBy: "test", now: new Date() });
      expect(await release(w)).toMatchObject({ released: 1 });
      await settle();
      expect(posts).toHaveLength(1);
      expect(JSON.stringify(posts)).not.toContain("Delivery digest"); // individually, not a second digest
    } finally { w.stop(); }
  });

  it("outside quiet the digest flush posts as today", async () => {
    prefs = { deliveryClass: "C", availability: "available" };
    const row = await park();
    const alert = await alertFor(row);
    repo.update({ qitemId: row.qitemId, actorSession: "daemon@kernel", transitionNote: `delivery-decision: digest window=4h notification_key=${alert.notificationKey ?? alert.qitemId}` });
    const w = wire();
    try {
      w.startServices?.();
      const flushed = await runDeliveryDigestFlush({ queueRepo: repo, registry: { loadHumanRegistry: () => registryWith(prefs) }, home, window: "4h",
        dispatch: (op, ref, payload, opts) => w.dispatcher.dispatch(op, ref, payload, opts as never) });
      await settle();
      expect(flushed.dispatched).toBe(1);
      expect(posts).toHaveLength(1);
    } finally { w.stop(); }
  });

  it("the operator rung's escalation still dispatches during quiet (it is always urgent)", async () => {
    quietNow();
    const row = await park();
    const dispatched: unknown[] = [];
    const engine = makeOperatorDeliveryEngine({ home, queueRepo: repo, registry: { loadHumanRegistry: () => registryWith(prefs) },
      dispatch: (_op, _ref, payload) => { dispatched.push(payload); return { ok: true, decisionId: "d" } as never; } });
    const out = await engine.dispatchEscalation(row, "test");
    expect(out).toMatchObject({ dispatched: true, decision: "interrupt" });
    expect(dispatched).toHaveLength(1);
  });
});

// ─── After QA: a buffered release re-checked at post time, and quiet per actual recipient in the digest flush ──────

describe("after QA: changed circumstances and a second person", () => {
  let db: Database.Database;
  let repo: QueueRepository;
  let home: string;
  let quiet: HumanQuietStore;
  let order: string[];
  const posts: Array<Record<string, unknown>> = [];
  const wires: Array<ReturnType<typeof buildSlackGatewayWire>> = [];
  let failPosts = false;
  const person = (id: string) => ({
    entityId: id, class: "human" as const, displayName: id, address: `${id}@external`,
    prefs: id === "person" ? { deliveryClass: "C", availability: "available" } : { deliveryClass: "B", availability: "available" },
    connectorBindings: [{ kind: "slack" as const, connectorRef: "primary", secretsRef: "env:SLACK_BOT_TOKEN", role: "primary" as const, handle: id === "person" ? "UPERSON" : "UOTHER" }],
  });
  const registry = () => ({ ok: true as const, entities: order.map(person) });

  beforeEach(() => {
    db = createDb();
    migrate(db, ALL_MIGRATIONS);
    order = ["person", "other"];
    repo = new QueueRepository(db, new EventBus(db), { validateRig: () => true, loadHumanRegistry: registry as never });
    home = mkdtempSync(join(tmpdir(), "quiet-qa-"));
    quiet = new HumanQuietStore(db);
    posts.length = 0;
    failPosts = false;
    const secrets = join(home, "slack.env");
    writeFileSync(secrets, "SLACK_BOT_TOKEN=xoxb-EXAMPLE-fake\n", { mode: 0o600 });
    saveConfig({ ...DEFAULT_CONFIG, enabled: true, channel: "C-OWNER", secretsEnvFile: secrets }, home);
  });
  afterEach(() => { for (const w of wires.splice(0)) w.stop(); db.close(); rmSync(home, { recursive: true, force: true }); });

  function wire() {
    const w = buildSlackGatewayWire({
      home, queueRepo: repo, registry: { loadHumanRegistry: registry as never, resolveSlackHandle }, outboundIntervalMs: 3_600_000,
      fetchImpl: async (url, init) => {
        if (String(url).includes("chat.postMessage")) {
          if (failPosts) throw new Error("connector down before the post");
          posts.push(JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>);
          return new Response(JSON.stringify({ ok: true, ts: `1724.${100 + posts.length}` }), { status: 200, headers: { "content-type": "application/json" } });
        }
        return new Response(JSON.stringify({ ok: true, messages: [] }), { status: 200, headers: { "content-type": "application/json" } });
      },
    });
    w.startServices?.();
    wires.push(w);
    return w;
  }
  let n = 0;
  const ask = (extra: Record<string, unknown> = {}) => repo.create({ sourceSession: "asker@r", destinationSession: "person@external",
    body: `Full question ${++n}: choose the window`, summary: `Choose ${n}`, evidenceRef: `/proof/${n}.md`, humanIntent: "decision", ...extra, nudge: false } as never) as Promise<QueueItem>;
  const alertFor = async (row: QueueItem) =>
    (await makeQueuePorts(repo, { loadHumanRegistry: registry } as never).listHumanAlerts({ minimumLevel: "RECORD" })).find((a) => a.qitemId === row.qitemId)!;
  const quietOn = () => { const now = new Date(); quiet.setUntil("person", { until: new Date(now.getTime() + 3_600_000), timezone: "UTC", setBy: "t", now }); };
  const notes = (row: QueueItem) => repo.listTransitions(row.qitemId).map((t) => t.transitionNote ?? "");
  const releaseJobs = () => new WatchdogJobsRepository(db).listActive().filter((j) => j.policy === QUIET_RELEASE_POLICY);

  it.each(["quiet back on", "posting threshold raised", "recipient changed"])(
    "a release dispatched but not yet posted is re-checked when it's replayed: %s posts nothing", async (change) => {
      quietOn();
      const row = await ask(change === "posting threshold raised" ? { humanIntent: "update" } : {});
      const first = wire();
      const alert = await alertFor(row);
      first.dispatcher.dispatch(OUTBOUND_OP, alert.destinationSession!, toPayload(alert));
      await settle();
      expect(posts).toHaveLength(0);
      quiet.end("person", { endedBy: "t", now: new Date() });
      // The release is dispatched while the connector is down: it waits in the durable buffer.
      const buffer = new DispatchBuffer(home);
      const [job] = releaseJobs();
      const released = await runQuietRelease({ queueRepo: repo, jobsRepo: new WatchdogJobsRepository(db), home, jobId: job!.jobId,
        registry: { loadHumanRegistry: registry as never },
        dispatch: (op, ref, payload, opts) => { buffer.enqueue({ kind: "outbound_decision", decisionId: opts!.decisionId!, op, entityBindingRef: ref, payload }); return { ok: true }; } });
      expect(released).toMatchObject({ state: "pending", released: 1 });
      expect(buffer.pending()).toHaveLength(1);
      if (change === "quiet back on") quietOn();
      else if (change === "posting threshold raised") saveConfig({ ...DEFAULT_CONFIG, enabled: true, channel: "C-OWNER", secretsEnvFile: join(home, "slack.env"), minimumLevelThatPosts: "ALERT" }, home);
      else db.prepare("UPDATE queue_items SET destination_session = ? WHERE qitem_id = ?").run("other@external", row.qitemId);
      first.stop();
      wire(); // restart: the gateway replays what's buffered
      await settle();
      expect(posts, `nothing posts after: ${change}`).toHaveLength(0);
      const expected = change === "quiet back on" ? "quiet-release-returned " : "quiet-release-disposition ";
      expect(notes(row).some((n) => n.startsWith(expected))).toBe(true);
      if (change === "quiet back on") expect(releaseJobs()).toHaveLength(1); // still held, still owned by the job
    });

  it("full flow: the real release fails to post, quiet comes back under a newer window, and the restart's replay posts nothing", async () => {
    quietOn();
    const row = await ask();
    const first = wire();
    const alert = await alertFor(row);
    first.dispatcher.dispatch(OUTBOUND_OP, alert.destinationSession!, toPayload(alert));
    await settle();
    quiet.end("person", { endedBy: "t", now: new Date() });
    failPosts = true;
    const [job] = releaseJobs();
    expect(await runQuietRelease({ queueRepo: repo, jobsRepo: new WatchdogJobsRepository(db), home, jobId: job!.jobId,
      registry: { loadHumanRegistry: registry as never },
      dispatch: (op, ref, payload, opts) => first.dispatcher.dispatch(op, ref, payload, opts as never) })).toMatchObject({ state: "pending", released: 1 });
    await settle();
    expect(posts).toHaveLength(0);
    expect(new DispatchBuffer(home).pending().filter((d) => d.decisionId.startsWith("quiet-release:"))).toHaveLength(1);
    quietOn(); // a newer quiet revision
    first.stop();
    repo = new QueueRepository(db, new EventBus(db), { validateRig: () => true, loadHumanRegistry: registry as never });
    failPosts = false;
    wire(); // restart: startup replays the buffered release
    await settle();
    expect(posts).toHaveLength(0);
    expect(notes(row).some((n) => n.startsWith("quiet-release-returned "))).toBe(true);
    expect(releaseJobs()).toHaveLength(1);
  });

  it("the digest flush holds a pre-quiet ask for a quiet second person, under that person's window", async () => {
    order = ["other", "person"];
    const row = await ask();
    const alert = await alertFor(row);
    repo.update({ qitemId: row.qitemId, actorSession: "daemon@kernel", transitionNote: `delivery-decision: digest window=4h notification_key=${alert.notificationKey}` });
    quietOn();
    const w = wire();
    await runDeliveryDigestFlush({ queueRepo: repo, registry: { loadHumanRegistry: registry as never }, home, window: "4h",
      dispatch: (op, ref, payload, opts) => w.dispatcher.dispatch(op, ref, payload, opts as never) });
    await settle();
    expect(posts).toHaveLength(0);
    expect(notes(row).some((n) => n.startsWith("delivery-decision: quiet-hold"))).toBe(true);
    expect(releaseJobs()).toHaveLength(1);
  });

  it("the digest flush doesn't capture a non-quiet person's ask under someone else's quiet window", async () => {
    const row = await ask({ destinationSession: "other@external" });
    const alert = await alertFor(row);
    repo.update({ qitemId: row.qitemId, actorSession: "daemon@kernel", transitionNote: `delivery-decision: digest window=4h notification_key=${alert.notificationKey}` });
    quietOn(); // the first registered person is quiet; the ask's recipient isn't
    const w = wire();
    await runDeliveryDigestFlush({ queueRepo: repo, registry: { loadHumanRegistry: registry as never }, home, window: "4h",
      dispatch: (op, ref, payload, opts) => w.dispatcher.dispatch(op, ref, payload, opts as never) });
    await settle();
    expect(posts).toHaveLength(1);
    expect(notes(row).some((n) => n.startsWith("delivery-decision: quiet-hold"))).toBe(false);
    expect(releaseJobs()).toHaveLength(0);
  });
});
