/**
 * Quorum's browser app. It drives the same deterministic Simulator the tests
 * and chaos harness use, so what you see is exactly the code under test.
 *
 * The page is built around one idea: five servers keep identical copies of a
 * log, and you can't make them disagree. A live status sentence explains what
 * the cluster is doing, a short guided story walks through the key moments,
 * and then the cluster is yours to break.
 */
import {
  type Action, BUG_DESCRIPTIONS, type Bugs, type Flight, generateScenario, PROPERTY_DESCRIPTIONS,
  type RaftNode, runScenario, type SimOptions, Simulator, type Violation,
} from "../src/index.js";

const $ = <T extends HTMLElement = HTMLElement>(id: string) => document.getElementById(id) as T;
const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const list = (ids: number[]) => ids.map((i) => `S${i}`).join(ids.length === 2 ? " and " : ", ").replace(/, (S\d)$/, " and $1");

/** Simulated ms per real ms at 1×: one real second shows 60 ms of cluster time. */
const BASE_SPEED = 0.06;
const VISUAL_NETWORK = { latencyMin: 12, latencyMax: 28 };

// ------------------------------------------------------------------ session

/** A simulation plus its timeline of actions. Rewinding rebuilds from the seed. */
class Session {
  sim: Simulator;
  timeline: { time: number; action: Action }[] = [];
  cursor = 0;
  maxTime = 0;
  constructor(readonly opts: SimOptions, readonly replay: { seed: number; bug: string; violation: Violation } | null = null) {
    this.sim = new Simulator({ ...opts });
  }
  advance(t: number): void {
    while (this.cursor < this.timeline.length && this.timeline[this.cursor].time <= t) {
      const step = this.timeline[this.cursor];
      this.sim.runUntil(step.time);
      if (!this.sim.checker.ok) break;
      this.sim.apply(step.action);
      this.cursor++;
    }
    if (this.sim.checker.ok) this.sim.runUntil(t);
    this.maxTime = Math.max(this.maxTime, this.sim.time);
  }
  rebuild(t: number): void {
    this.sim = new Simulator({ ...this.opts });
    this.cursor = 0;
    this.advance(t);
  }
  act(action: Action): void {
    this.timeline.length = this.cursor;
    this.timeline.push({ time: this.sim.time, action });
    this.cursor++;
    this.sim.apply(action);
    this.maxTime = this.sim.time;
  }
  nextCommand(): string {
    return "x" + (this.timeline.filter((s) => s.action.kind === "submit").length + 1);
  }
}

const freshSession = () => new Session({ seed: Math.floor(Math.random() * 1e9), network: { ...VISUAL_NETWORK } });

let session = freshSession();
let playing = true;
let speed = 1;
let selected: number | null = null;
let lossPct = 0;

// ------------------------------------------------------------------ reading the cluster

const live = (sim: Simulator) => sim.nodes.filter((n): n is RaftNode => n !== null);
const commandsCommitted = (sim: Simulator) => Math.max(0, ...live(sim).map((n) => n.applied.filter((c) => c !== null).length));

/** Groups of running servers that can all reach each other. */
function components(sim: Simulator): number[][] {
  const up = sim.ids.filter((id) => sim.isUp(id));
  const seen = new Set<number>();
  const groups: number[][] = [];
  for (const start of up) {
    if (seen.has(start)) continue;
    const group: number[] = [];
    const stack = [start];
    seen.add(start);
    while (stack.length) {
      const a = stack.pop()!;
      group.push(a);
      for (const b of up) {
        if (!seen.has(b) && sim.links[a - 1][b - 1] && sim.links[b - 1][a - 1]) { seen.add(b); stack.push(b); }
      }
    }
    groups.push(group.sort());
  }
  return groups;
}

/** The leader nobody has replaced yet, and any leaders that are out of date. */
function leaders(sim: Simulator): { current: RaftNode | null; stale: RaftNode[] } {
  const all = live(sim).filter((n) => n.role === "leader");
  const top = Math.max(0, ...live(sim).map((n) => n.currentTerm));
  const current = sim.leader();
  return { current: current && current.currentTerm === top ? current : current, stale: all.filter((n) => n !== current || n.currentTerm < top) };
}

const CONSEQUENCES: Record<string, string> = {
  "Election Safety": "Two servers both believe they lead the same term, so they can accept conflicting writes.",
  "Log Matching": "Two servers' copies of the log disagree about the past.",
  "Leader Completeness": "A server took charge while missing data the cluster had already promised to keep. As leader it will overwrite that data everywhere: it's lost.",
  "State Machine Safety": "Two servers executed different writes at the same position, so clients see different data depending on which server they ask.",
};

function involved(v: Violation | undefined): { nodes: Set<number>; index: number | null } {
  if (!v) return { nodes: new Set(), index: null };
  const nodes = new Set([...v.detail.matchAll(/S(\d)/g)].map((m) => Number(m[1])));
  const idx = v.detail.match(/(?:entry|index) (\d+)/);
  return { nodes, index: idx ? Number(idx[1]) : null };
}

/** One plain-English sentence (plus detail) describing the cluster right now. */
function describe(sim: Simulator): { tone: "" | "warn" | "bad"; headline: string; detail: string } {
  const v = sim.checker.violations[0];
  if (v) {
    return { tone: "bad", headline: `${v.property} broken: the servers now disagree`,
      detail: `${esc(CONSEQUENCES[v.property] ?? "")} <code>${esc(v.detail)}</code>` };
  }
  const up = sim.ids.filter((id) => sim.isUp(id));
  const down = sim.ids.filter((id) => !sim.isUp(id));
  const groups = components(sim);
  const { current, stale } = leaders(sim);
  const writes = commandsCommitted(sim);
  const writesText = writes === 0 ? "No writes committed yet." : `${writes} write${writes === 1 ? " is" : "s are"} committed and identical on every server that has them.`;

  if (up.length < 3) {
    return { tone: "bad", headline: `Only ${up.length} of 5 servers are running`,
      detail: `Without a majority (3), no leader can be elected and nothing can be committed. The cluster stops rather than risk disagreeing. Restart a server to recover.` };
  }
  if (groups.length > 1) {
    const major = groups.find((g) => g.length >= 3);
    const minors = groups.filter((g) => g !== major);
    const strandedLeader = stale.find((n) => minors.some((g) => g.includes(n.id))) ?? (current && minors.some((g) => g.includes(current.id)) ? current : null);
    let detail = minors.map((g) => `${list(g)} can't reach a majority`).join("; ") + ".";
    if (strandedLeader) detail += ` S${strandedLeader.id} still thinks it's the leader, but nothing it accepts can ever be committed.`;
    if (major) {
      const ml = current && major.includes(current.id) && current !== strandedLeader ? current : null;
      detail += ml ? ` ${list(major)} have a majority and elected S${ml.id}: they carry on.` : ` ${list(major)} have a majority and are electing a leader.`;
    } else detail += " No side has a majority, so nothing can be committed until the network heals.";
    return { tone: "warn", headline: "The network is split", detail };
  }
  const candidates = live(sim).filter((n) => n.role === "candidate");
  if (!current) {
    if (candidates.length) {
      const c = candidates.reduce((a, b) => (b.votes.size > a.votes.size ? b : a));
      return { tone: "warn", headline: `Election for term ${c.currentTerm}: S${c.id} has ${c.votes.size} of the 3 votes it needs`,
        detail: candidates.length > 1 ? `${list(candidates.map((n) => n.id))} are all asking at once, so the vote may split. If nobody wins, their timers run out again and they retry.` : "Each server votes for at most one candidate per term, so there can only be one winner." };
    }
    return { tone: "warn", headline: "No leader yet",
      detail: "Each server is waiting a random time, shown by the ring around it. The first whose timer runs out will ask the others to vote for it." };
  }
  if (stale.length) {
    return { tone: "warn", headline: `S${current.id} leads term ${current.currentTerm}; S${stale[0].id} hasn't heard yet`,
      detail: `S${stale[0].id} still thinks it's leader of an older term. The next message it gets carries the newer term, and it will step down.` };
  }
  return { tone: "", headline: `S${current.id} is the leader (term ${current.currentTerm})`,
    detail: `${writesText} ${down.length ? `${list(down)} ${down.length === 1 ? "is" : "are"} down, but the other ${up.length} still form a majority.` : "Writes go to the leader and become permanent once 3 of 5 servers store them."}` };
}

// ------------------------------------------------------------------ the picture

const CX = 320, CY = 300, RING = 215, R = 44;
const pos = (id: number) => {
  const a = -Math.PI / 2 + ((id - 1) * 2 * Math.PI) / 5;
  return { x: CX + RING * Math.cos(a), y: CY + RING * Math.sin(a) };
};
const COLORS = { follower: "#6A7B91", candidate: "#F5B84A", leader: "#3DD6B5", down: "#2B333E", danger: "#FF6464" };
const termColor = (term: number) => `hsl(${(term * 67 + 160) % 360} 55% 66%)`;

function drawStage(): void {
  const sim = session.sim;
  const t = sim.time;
  let svg = "";
  for (let a = 1; a <= 5; a++) for (let b = a + 1; b <= 5; b++) {
    const p = pos(a), q = pos(b);
    const up = sim.links[a - 1][b - 1];
    svg += `<line class="hit" data-link="${a}-${b}" x1="${p.x}" y1="${p.y}" x2="${q.x}" y2="${q.y}"><title>Click to ${up ? "cut" : "restore"} the link between S${a} and S${b}</title></line>`;
    svg += `<line class="link${up ? "" : " cut"}" x1="${p.x}" y1="${p.y}" x2="${q.x}" y2="${q.y}"/>`;
  }
  for (const f of sim.inFlight() as Flight[]) {
    const prog = (t - f.sentAt) / (f.deliverAt - f.sentAt || 1);
    if (prog < 0 || prog > 1 || (f.dropped && prog > 0.6)) continue;
    const p = pos(f.from), q = pos(f.to);
    const dx = q.x - p.x, dy = q.y - p.y, len = Math.hypot(dx, dy), ux = dx / len, uy = dy / len;
    const x = p.x + ux * R + (dx - 2 * ux * R) * prog, y = p.y + uy * R + (dy - 2 * uy * R) * prog;
    const m = f.msg;
    let r = 4, fill = COLORS.leader, stroke = "none";
    if (m.type === "RequestVote") { r = 6.5; fill = COLORS.candidate; }
    else if (m.type === "RequestVoteReply") { r = 5; fill = m.voteGranted ? COLORS.candidate : "none"; stroke = COLORS.candidate; }
    else if (m.type === "AppendEntries") r = m.entries.length ? 7.5 : 4.5;
    else { fill = "none"; stroke = COLORS.leader; }
    const op = f.dropped ? Math.max(0, 1 - prog * 1.6) : 1;
    svg += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${r}" fill="${fill}" stroke="${f.dropped ? COLORS.danger : stroke}" stroke-width="1.6" opacity="${op.toFixed(2)}"/>`;
  }
  const bad = involved(sim.checker.violations[0]).nodes;
  const { stale } = leaders(sim);
  for (const id of sim.ids) {
    const node = sim.node(id);
    const { x, y } = pos(id);
    const role = node ? node.role : "down";
    const isStale = !!node && stale.includes(node);
    svg += `<g class="node ${role}" data-node="${id}" transform="translate(${x} ${y})"><title>S${id}: ${node ? role : "crashed"}. Click for details.</title>`;
    if (selected === id) svg += `<circle class="sel" r="${R + 17}"/>`;
    if (bad.has(id)) svg += `<circle class="culprit" r="${R + 20}"/>`;
    if (node && role !== "leader") {
      const frac = Math.max(0, Math.min(1, (node.electionDeadline - t) / node.electionTimeout));
      const c = 2 * Math.PI * (R + 8);
      svg += `<circle r="${R + 8}" fill="none" stroke="${COLORS.follower}" stroke-opacity="0.25" stroke-width="4"/>`;
      svg += `<circle r="${R + 8}" fill="none" stroke="${COLORS[role]}" stroke-width="4" stroke-linecap="round" stroke-dasharray="${(frac * c).toFixed(1)} ${c.toFixed(1)}" transform="rotate(-90)"/>`;
    } else if (node) {
      svg += `<circle r="${R + 8}" fill="none" stroke="${COLORS.leader}" stroke-width="2.5" opacity="0.55"/>`;
    }
    svg += `<circle r="${R}" fill="${COLORS[role]}"${isStale ? ` fill-opacity="0.4"` : ""}${node ? "" : ` stroke="#5A6676" stroke-dasharray="4 4"`}/>`;
    svg += `<text class="label" y="7">S${id}</text>`;
    if (!node) svg += `<path d="M-14 -16 L14 12 M14 -16 L-14 12" stroke="${COLORS.danger}" stroke-width="3" opacity="0.8"/>`;
    let sub = node ? `term ${node.currentTerm}` : "crashed";
    if (node?.role === "candidate") sub += ` · ${node.votes.size}/3 votes`;
    if (isStale) sub += " · stale";
    svg += `<text class="term" y="${R + 30}">${sub}</text></g>`;
  }
  $("stage").innerHTML = svg;
}

function drawLogs(): void {
  const sim = session.sim;
  const logs = sim.ids.map((id) => sim.logOf(id));
  const longest = Math.max(0, ...logs.map((l) => l.length - 1));
  const shown = 11;
  const start = Math.max(1, longest - shown + 1);
  const end = Math.max(longest, start + shown - 1);
  const clusterCommit = Math.max(0, ...live(sim).map((n) => n.commitIndex));
  const focus = involved(sim.checker.violations[0]).index;
  let html = `<div class="lrow idx"><span class="who"></span>`;
  for (let i = start; i <= end; i++) html += `<span class="c${i === clusterCommit ? " commitline" : ""}">#${i}</span>`;
  html += `</div>`;
  sim.ids.forEach((id, k) => {
    const node = sim.node(id);
    const role = node ? node.role : "down";
    html += `<div class="lrow${node ? "" : " down"}"><span class="who"><i style="background:${COLORS[role]}"></i>S${id}</span>`;
    const log = logs[k];
    for (let i = start; i < log.length; i++) {
      const e = log[i];
      const col = termColor(e.term);
      const open = !node || i > node.commitIndex;
      const label = e.command === null ? "—" : e.command;
      const style = open ? `border-color:${col};color:${col}` : `background:${col};color:#0B0F14`;
      html += `<span class="c${open ? " open" : ""}${i === focus ? " focus" : ""}${i === clusterCommit ? " commitline" : ""}" style="${style}" title="Entry #${i}: ${esc(e.command === null ? "no-op written by a new leader" : `"${e.command}"`)}, written in term ${e.term}. ${open ? "Not committed on this server yet." : "Committed."}">${esc(label)}<small>t${e.term}</small></span>`;
    }
    html += `</div>`;
  });
  // The promise, checked live: every server that has committed a position holds
  // the same entry there. (A server that hasn't committed it yet may still hold
  // a different, uncommitted entry: that one will be overwritten, and that's safe.)
  let agree = sim.checker.violations.length === 0;
  for (let i = 1; i <= clusterCommit && agree; i++) {
    const seen = live(sim).filter((n) => n.commitIndex >= i).map((n) => `${n.log[i].term}:${n.log[i].command}`);
    agree = seen.every((x) => x === seen[0]);
  }
  html += clusterCommit > 0
    ? `<p class="agree${agree ? "" : " no"}">${agree
        ? `✓ Entries #1–#${clusterCommit} are committed, and every server that has committed them holds identical copies.`
        : "✗ Committed entries no longer match: the servers disagree."}</p>`
    : `<p class="agree" style="color:var(--muted)">Nothing committed yet.</p>`;
  $("logs").innerHTML = html;
}

let feedKey = "";
function drawFeed(): void {
  const trace = session.sim.trace;
  const key = `${trace.length}:${trace[trace.length - 1]?.time ?? 0}`;
  if (key === feedKey) return;
  feedKey = key;
  $("feed").innerHTML = trace.slice(-100).reverse()
    .map((e) => `<li class="${e.kind}"><time>${(e.time / 1000).toFixed(3)}s</time><span>${esc(e.text)}</span></li>`).join("");
}

function drawStatus(): void {
  const s = describe(session.sim);
  $("status").className = `status ${s.tone}`;
  $("headline").textContent = s.headline;
  $("detail").innerHTML = s.detail;
}

function drawTransport(): void {
  const sim = session.sim;
  $("clock").textContent = `${(sim.time / 1000).toFixed(3)} s`;
  const tl = $<HTMLInputElement>("timeline");
  tl.max = String(session.maxTime);
  if (document.activeElement !== tl) tl.value = String(sim.time);
  $("play").textContent = playing ? "❚❚" : "▶";
  $("play").setAttribute("aria-label", playing ? "Pause" : "Play");
}

// ------------------------------------------------------------------ the guided story

interface StoryState {
  step: number;
  crashed: number | null;
  minority: number[] | null;
  majority: number[] | null;
  strandedLeader: number | null;
  minorityWrite: string | null;
  majorityWrite: string | null;
  healedAt: number | null;
}
const STEPS = 5;
let mode: "story" | "sandbox" | "replay" = "story";
let story: StoryState = newStory();
function newStory(): StoryState {
  return { step: 0, crashed: null, minority: null, majority: null, strandedLeader: null, minorityWrite: null, majorityWrite: null, healedAt: null };
}

function stepDone(): boolean {
  const sim = session.sim;
  const leader = sim.leader();
  switch (story.step) {
    case 0: return !!leader;
    case 1: return commandsCommitted(sim) >= 3;
    case 2: return story.crashed !== null && !!leader && leader.id !== story.crashed;
    case 3: return story.healedAt !== null && leaders(sim).stale.length === 0 && !!leader && sim.time - story.healedAt > 150;
    default: return true;
  }
}

function btn(label: string, action: string, cls = "", disabled = false): string {
  return `<button class="btn ${cls}" data-do="${action}"${disabled ? " disabled" : ""}>${label}</button>`;
}

function storyHtml(): string {
  const sim = session.sim;
  const leader = sim.leader();
  const done = stepDone();
  let h = `<div class="steps">${Array.from({ length: STEPS }, (_, i) => `<i class="${i < story.step ? "done" : i === story.step ? "now" : ""}"></i>`).join("")}</div>`;
  h += `<p class="kicker">Step ${story.step + 1} of ${STEPS}</p>`;
  switch (story.step) {
    case 0:
      h += `<h2>Five servers, one log</h2>
        <p>Each server keeps its own copy of a log, a list of writes, shown on the right. Raft's whole job is to keep those copies identical, even when servers crash or messages get lost.</p>
        <p>First they need a leader. The ring around each server is its election timer. When one runs out, that server asks the others to vote for it.</p>`;
      h += done ? `<p class="got">S${leader!.id} won. A candidate needs votes from a majority, 3 of 5, and each server votes only once per term, so there can only ever be one winner.</p>`
        : `<p class="wait">Waiting for a timer to run out…</p>`;
      break;
    case 1: {
      const n = commandsCommitted(sim);
      h += `<h2>Write some data</h2>
        <p>Clients send writes to the leader, which copies them to the others. As soon as a majority has stored a write, it's <strong>committed</strong>: permanent, whatever fails next.</p>
        <div class="actions">${btn(`Write a value to S${leader?.id ?? "?"}`, "write", "primary", !leader)}</div>
        <p>Watch the new entry appear outlined on the leader's row, spread to the other rows, then turn solid.</p>`;
      h += done ? `<p class="got">${n} writes committed. Look at the solid entries: they're identical on every server. That's the promise.</p>`
        : `<p class="wait">${n} of 3 writes committed</p>`;
      break;
    }
    case 2:
      h += `<h2>Crash the leader</h2>
        <p>What if the leader dies? The followers stop hearing its heartbeats, a timer runs out, and the survivors elect a new leader.</p>
        <div class="actions">${btn(story.crashed ? `S${story.crashed} has crashed` : `Crash S${leader?.id ?? "?"}, the leader`, "crash-leader", "danger", !!story.crashed || !leader)}</div>`;
      if (done) h += `<p class="got">S${leader!.id} took over. Nothing committed was lost: a server only gets a vote from servers whose logs aren't ahead of its own, so a new leader always has every committed write.</p>`;
      else if (story.crashed) h += `<p class="wait">Waiting for the survivors to elect a new leader…</p>`;
      break;
    case 3: {
      h += `<h2>Split the network</h2>`;
      if (!story.minority) {
        h += `<p>Now cut the network in two: the leader plus one follower on one side, the other three on the other. Then write to both sides. Which write survives?</p>
          <div class="actions">${btn("Split the network", "split", "primary", !leader)}</div>`;
        if (!leader) h += `<p class="wait">Waiting for a leader…</p>`;
        break;
      }
      const majorityLeader = leader && story.majority!.includes(leader.id) ? leader : null;
      if (story.healedAt === null) {
        h += `<p>S${story.strandedLeader} is stranded with only ${list(story.minority!.filter((i) => i !== story.strandedLeader))}. It doesn't know that: it still thinks it's leader. The other three can still form a majority.</p>
          <div class="actions">
            ${btn(story.minorityWrite ? `Wrote "${story.minorityWrite}" to S${story.strandedLeader}` : `Write to S${story.strandedLeader} (the stranded side)`, "write-minority", "", !!story.minorityWrite)}
            ${btn(story.majorityWrite ? `Wrote "${story.majorityWrite}" to S${majorityLeader?.id ?? "?"}` : majorityLeader ? `Write to S${majorityLeader.id} (the majority side)` : "Majority side is electing a leader…", "write-majority", "", !!story.majorityWrite || !majorityLeader)}
          </div>`;
        if (story.minorityWrite && story.majorityWrite) {
          h += `<p>Both servers accepted a write. Only the majority's can commit: look at the logs. Now reconnect them.</p><div class="actions">${btn("Heal the network", "heal", "primary")}</div>`;
        } else h += `<p class="wait">Send a write to each side</p>`;
      } else {
        const lost = story.minorityWrite && !sim.ids.some((id) => sim.logOf(id).some((e) => e.command === story.minorityWrite));
        h += done
          ? `<p class="got">S${story.strandedLeader} heard about the newer term and stepped down. ${lost ? `Its write "${esc(story.minorityWrite!)}" was erased from every log. That's safe: it never reached a majority, so it was never committed and no client was told it succeeded.` : "Its uncommitted write is being overwritten."} The majority's write "${esc(story.majorityWrite ?? "")}" is permanent.</p>`
          : `<p class="wait">Reconnecting… watch S${story.strandedLeader}'s uncommitted entry</p>`;
      }
      break;
    }
    case 4:
      h += `<h2>Now try to break it</h2>
        <p>That's the core of Raft: every decision needs a majority, and any two majorities share at least one server, so nothing committed can ever be forgotten.</p>
        <p>The cluster is yours. Crash servers, cut links by clicking the lines between them, or turn up message loss. You can't make the committed entries disagree.</p>
        <div class="actions">${btn("See how it's tested, 10,000 times →", "goto-break", "primary")}</div>`;
      break;
  }
  if (story.step < STEPS - 1) {
    h += `<div class="nav">${btn("Skip to the sandbox", "skip", "link")}${btn("Next →", "next", "primary", !done)}</div>`;
  } else {
    h += sandboxHtml();
  }
  return h;
}

function sandboxHtml(): string {
  return `<div class="sandbox-grid">
      ${btn("Write a value", "write", "primary")}
      ${btn("Crash the leader", "crash-leader", "danger")}
      ${btn("Split the network", "split")}
      ${btn("Heal network", "heal")}
      ${btn("Restart all", "restart-all")}
    </div>
    <label class="slider">Message loss <output id="lossOut"></output><input type="range" id="loss" min="0" max="50" step="5"></label>
    <div class="nav">${btn("Start the story again", "restart-story", "link")}${btn("New cluster", "fresh", "link")}</div>`;
}

function guideHtml(): string {
  if (mode === "replay" && session.replay) {
    const r = session.replay;
    return `<p class="kicker">Replay · seed ${r.seed}</p><h2>Watching a bug break Raft</h2>
      <p>This run has a deliberate bug switched on:</p><p><strong>${esc(r.bug)}</strong></p>
      <p>The chaos test caught it at ${(r.violation.time / 1000).toFixed(3)} s. Press play to watch the last moments in slow motion. The servers involved will be circled in red.</p>
      <div class="actions">${btn("Back to Break it", "goto-break")}${btn("Back to a healthy cluster", "fresh")}</div>`;
  }
  if (mode === "sandbox") {
    return `<p class="kicker">Sandbox</p><h2>The cluster is yours</h2>
      <p>Click a server for details. Click the line between two servers to cut or restore that link.</p>${sandboxHtml()}`;
  }
  return storyHtml();
}

let lastGuide = "";
function drawGuide(): void {
  const html = guideHtml();
  if (html !== lastGuide) {
    lastGuide = html;
    $("guide").innerHTML = html + `<div id="serverCard"></div>`;
    const loss = document.getElementById("loss") as HTMLInputElement | null;
    if (loss) loss.value = String(lossPct);
  }
  const out = document.getElementById("lossOut");
  if (out) out.textContent = `${lossPct}%`;
  drawServerCard();
}

function drawServerCard(): void {
  const box = document.getElementById("serverCard");
  if (!box) return;
  const showCard = mode !== "story" || story.step === STEPS - 1;
  const sim = session.sim;
  const id = selected ?? sim.leader()?.id ?? null;
  if (!showCard || id === null) { box.innerHTML = ""; return; }
  const node = sim.node(id);
  let h = `<div class="server-card"><div class="head"><strong>S${id}</strong><span class="badge ${node ? node.role : "down"}">${node ? node.role : "crashed"}</span>${selected === null ? `<span class="hint" style="margin:0">the leader</span>` : ""}</div>`;
  if (!node) {
    h += `<p class="hint">Its term, vote and log are safe on disk; everything else was lost.</p>${btn(`Restart S${id}`, "restart", "primary")}</div>`;
    box.innerHTML = h;
    return;
  }
  const rows: [string, string][] = [
    ["Term", String(node.currentTerm)],
    ["Voted for", node.votedFor === null ? "nobody" : `S${node.votedFor}`],
    ["Log / committed", `${node.lastLogIndex()} / ${node.commitIndex}`],
  ];
  if (node.role !== "leader") rows.push(["Timer", `${Math.max(0, node.electionDeadline - sim.time)} ms`]);
  else rows.push(["Followers at", node.peers.map((p) => `S${p}:${node.matchIndex.get(p)}`).join(" ")]);
  h += `<dl class="kv">${rows.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("")}</dl>`;
  h += `<div class="sandbox-grid" style="margin:0">${btn(`Crash S${id}`, "crash", "danger")}${btn(`Isolate S${id}`, "isolate")}</div></div>`;
  box.innerHTML = h;
}

// ------------------------------------------------------------------ actions

function act(action: Action): void {
  session.act(action);
  drawAll();
}

function splitNetwork(): void {
  const sim = session.sim;
  const leader = sim.leader();
  const ids = sim.ids.filter((i) => sim.isUp(i));
  const lead = leader?.id ?? ids[0];
  const minority = [lead, ids.find((i) => i !== lead)!];
  const majority = sim.ids.filter((i) => !minority.includes(i));
  act({ kind: "partition", groups: [minority, majority] });
  return void Object.assign(story, { minority, majority, strandedLeader: lead });
}

$("guide").addEventListener("click", (e) => {
  const what = (e.target as Element).closest("[data-do]")?.getAttribute("data-do");
  if (!what) return;
  const sim = session.sim;
  const leader = sim.leader();
  playing = true;
  switch (what) {
    case "next":
      story.step++;
      if (story.step === 3) for (const id of sim.ids) if (!sim.isUp(id)) act({ kind: "restart", node: id });
      if (story.step === STEPS - 1) mode = "story";
      break;
    case "skip": mode = "sandbox"; break;
    case "restart-story": restart(); break;
    case "fresh": restart(); break;
    case "goto-break": showTab("break"); break;
    case "write": act({ kind: "submit", command: session.nextCommand() }); break;
    case "crash-leader":
      if (leader) { story.crashed ??= leader.id; act({ kind: "crash", node: leader.id }); }
      break;
    case "split":
      if (mode === "story" && story.step === 3) splitNetwork();
      else {
        const ids = [...sim.ids];
        const minority = leader ? [leader.id, ids.find((i) => i !== leader.id)!] : ids.slice(0, 2);
        act({ kind: "partition", groups: [minority, ids.filter((i) => !minority.includes(i))] });
      }
      break;
    case "write-minority": {
      const cmd = session.nextCommand();
      story.minorityWrite = cmd;
      act({ kind: "submit", command: cmd, node: story.strandedLeader! });
      break;
    }
    case "write-majority": {
      const cmd = session.nextCommand();
      story.majorityWrite = cmd;
      act({ kind: "submit", command: cmd });
      break;
    }
    case "heal":
      act({ kind: "heal" });
      if (mode === "story" && story.step === 3) story.healedAt = session.sim.time;
      break;
    case "restart-all": for (const id of sim.ids) if (!sim.isUp(id)) act({ kind: "restart", node: id }); break;
    case "crash": if (selected ?? leader) act({ kind: "crash", node: selected ?? leader!.id }); break;
    case "restart": if (selected) act({ kind: "restart", node: selected }); break;
    case "isolate": {
      const id = selected ?? leader?.id;
      if (id) act({ kind: "partition", groups: [[id], sim.ids.filter((i) => i !== id)] });
      break;
    }
  }
  drawAll();
});
$("guide").addEventListener("input", (e) => {
  const el = e.target as HTMLInputElement;
  if (el.id !== "loss") return;
  lossPct = Number(el.value);
  session.act({ kind: "setDropRate", rate: lossPct / 100 });
  drawGuide();
});

function restart(): void {
  session = freshSession();
  story = newStory();
  mode = "story";
  selected = null;
  lossPct = 0;
  playing = true;
  speed = 1;
  ($("speed") as HTMLSelectElement).value = "1";
  showTab("explore");
  drawAll();
}

// pointerdown, not click: the stage is redrawn every frame, so a click's target
// would be replaced between press and release and the click would be lost
$("stage").addEventListener("pointerdown", (e) => {
  const target = e.target as Element;
  const node = target.closest("[data-node]");
  if (node) {
    const id = Number(node.getAttribute("data-node"));
    selected = selected === id ? null : id;
    drawAll();
    return;
  }
  const link = target.closest("[data-link]");
  if (link) {
    const [a, b] = link.getAttribute("data-link")!.split("-").map(Number);
    act({ kind: "toggleLink", a, b });
  }
});
$("play").addEventListener("click", () => { playing = !playing; drawTransport(); });
$("speed").addEventListener("change", (e) => { speed = Number((e.target as HTMLSelectElement).value); });
let scrubQueued = false;
$("timeline").addEventListener("input", () => {
  playing = false;
  if (scrubQueued) return;
  scrubQueued = true;
  requestAnimationFrame(() => {
    scrubQueued = false;
    session.rebuild(Number(($("timeline") as HTMLInputElement).value));
    drawAll();
  });
});
document.addEventListener("keydown", (e) => {
  if ((e.target as Element).closest("input, select, textarea, button")) return;
  if (e.key === " ") { e.preventDefault(); playing = !playing; drawTransport(); }
});

// ------------------------------------------------------------------ loop

let lastFrame = performance.now();
let lastPanels = 0;
function drawAll(): void {
  drawStage(); drawLogs(); drawFeed(); drawStatus(); drawTransport(); drawGuide();
}
function frame(now: number): void {
  const dt = Math.min(100, now - lastFrame);
  lastFrame = now;
  if (playing && !$("explore").hidden) {
    if (!session.sim.checker.ok) playing = false;
    else session.advance(session.sim.time + dt * BASE_SPEED * speed);
  }
  drawStage();
  if (now - lastPanels > 150) {
    lastPanels = now;
    drawLogs(); drawFeed(); drawStatus(); drawTransport(); drawGuide();
  }
  requestAnimationFrame(frame);
}

// ------------------------------------------------------------------ tabs

function showTab(name: string): void {
  document.querySelectorAll<HTMLButtonElement>(".tabs button").forEach((b) => b.setAttribute("aria-selected", String(b.dataset.tab === name)));
  document.querySelectorAll<HTMLElement>(".page").forEach((p) => (p.hidden = p.id !== name));
  window.scrollTo(0, 0);
}
document.querySelectorAll<HTMLButtonElement>(".tabs button").forEach((b) => b.addEventListener("click", () => showTab(b.dataset.tab!)));

// ------------------------------------------------------------------ break it

const BUG_STORIES: Record<string, string> = {
  voteWithoutLogCheck: "A server votes for any candidate, even one missing data the cluster already committed.",
  acceptStaleLeader: "A server takes orders from a leader whose term is out of date.",
  commitOldTerms: "A leader declares old entries committed just by counting copies: the famous Figure 8 bug from the Raft paper.",
  forgetVoteOnRestart: "A server forgets who it voted for when it restarts, so it can vote twice in one election.",
};
const bugList = $("bugList");
for (const key of Object.keys(BUG_DESCRIPTIONS)) {
  bugList.insertAdjacentHTML("beforeend",
    `<label><input type="radio" name="bug" value="${key}"><span><strong>With a bug:</strong> ${esc(BUG_STORIES[key] ?? BUG_DESCRIPTIONS[key as keyof Bugs])}<br><small>Expect: caught, with a seed you can replay</small></span></label>`);
}
const WHY: Record<string, string> = {
  "Election Safety": "Otherwise two leaders could accept conflicting writes.",
  "Log Matching": "Otherwise servers could disagree about history.",
  "Leader Completeness": "Otherwise a new leader could erase committed data.",
  "State Machine Safety": "Otherwise clients would see different data on different servers.",
};
$("props").innerHTML = Object.entries(PROPERTY_DESCRIPTIONS)
  .map(([k, v]) => `<dt>${k}</dt><dd>${esc(v)}<em>${WHY[k] ?? ""}</em></dd>`).join("");

$("runChaos").addEventListener("click", () => {
  const runs = Math.max(1, Math.min(100000, Number(($("runs") as HTMLInputElement).value) || 1000));
  const first = Math.max(1, Number(($("firstSeed") as HTMLInputElement).value) || 1);
  const bugKey = (document.querySelector<HTMLInputElement>('input[name="bug"]:checked')?.value ?? "") as keyof Bugs | "";
  const bugs: Bugs = bugKey ? { [bugKey]: true } : {};
  const runBtn = $<HTMLButtonElement>("runChaos");
  const bar = $("bar");
  runBtn.disabled = true;
  bar.className = "";
  let done = 0, messages = 0, simMs = 0;
  const started = performance.now();
  const step = (): void => {
    const until = Math.min(runs, done + 20);
    for (; done < until; done++) {
      const r = runScenario(first + done, bugs);
      messages += r.stats.messagesSent;
      simMs += r.simulatedMs;
      if (r.violations.length) { done++; return finish(r.seed, r.violations[0]); }
    }
    bar.style.width = `${(done / runs) * 100}%`;
    $("results").innerHTML = `<p class="stat">Running scenario ${done.toLocaleString()} of ${runs.toLocaleString()}…</p>`;
    if (done < runs) setTimeout(step, 0);
    else finish(null, null);
  };
  const finish = (seed: number | null, v: Violation | null): void => {
    runBtn.disabled = false;
    const secs = ((performance.now() - started) / 1000).toFixed(1);
    const stats = `<p class="stat">${done.toLocaleString()} scenarios in ${secs} s · ${(simMs / 60000).toFixed(1)} minutes of simulated cluster time · ${messages.toLocaleString()} messages</p>`;
    if (seed === null || !v) {
      bar.style.width = "100%";
      $("results").innerHTML = `<p class="result-ok">No violations. All four guarantees held in every scenario.</p>${stats}` +
        (bugKey ? `<p class="hint">This bug survived this batch. Rare bugs need more scenarios: try 10,000.</p>` : "");
      return;
    }
    bar.className = "bad";
    bar.style.width = "100%";
    $("results").innerHTML = `<p class="result-bad">Caught on scenario ${done.toLocaleString()} (seed ${seed}).</p>${stats}
      <div class="violation"><strong>${esc(v.property)} broken</strong> at ${(v.time / 1000).toFixed(3)} s<br><code>${esc(v.detail)}</code></div>
      <button class="btn primary big" id="replay">Watch it happen</button>`;
    $("replay").addEventListener("click", () => replay(seed, bugs, bugKey ? BUG_STORIES[bugKey] ?? "" : "", v));
  };
  step();
});

function replay(seed: number, bugs: Bugs, bug: string, v: Violation): void {
  const scenario = generateScenario(seed);
  const s = new Session({ seed, network: scenario.network, raft: scenario.raft, bugs }, { seed, bug, violation: v });
  s.timeline = scenario.steps.slice();
  s.rebuild(Math.max(0, v.time - 150));
  session = s;
  mode = "replay";
  selected = null;
  playing = false;
  speed = 0.5;
  ($("speed") as HTMLSelectElement).value = "0.5";
  showTab("explore");
  drawAll();
}

drawAll();
requestAnimationFrame(frame);
