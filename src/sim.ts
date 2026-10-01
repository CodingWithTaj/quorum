/**
 * A deterministic simulation of a Raft cluster.
 *
 * Time is virtual (milliseconds), the network is simulated, and all
 * randomness comes from one seed. Given the same seed and the same
 * actions at the same times, every run is identical, so any bug the
 * chaos harness finds can be replayed exactly.
 */
import { MinHeap } from "./heap.js";
import { SafetyChecker } from "./invariants.js";
import { type NodeEvent, RaftNode } from "./raft.js";
import { Rng } from "./rng.js";
import { type Bugs, DEFAULT_CONFIG, type Message, type NodeId, type PersistentState, type RaftConfig } from "./types.js";

export interface NetworkConfig {
  latencyMin: number;
  latencyMax: number;
  dropRate: number;
  duplicateRate: number;
}

export interface SimOptions {
  nodes?: number;
  seed?: number;
  raft?: Partial<RaftConfig>;
  network?: Partial<NetworkConfig>;
  bugs?: Bugs;
  /** check Log Matching every N events (it compares whole logs) */
  logCheckEvery?: number;
}

export type Action =
  | { kind: "crash"; node: NodeId }
  | { kind: "restart"; node: NodeId }
  | { kind: "partition"; groups: NodeId[][] }
  | { kind: "heal" }
  | { kind: "toggleLink"; a: NodeId; b: NodeId }
  | { kind: "setDropRate"; rate: number }
  /** a client write, sent to `node` if given (it must think it's leader), otherwise to the current leader */
  | { kind: "submit"; command: string; node?: NodeId };

export interface Flight {
  id: number;
  from: NodeId;
  to: NodeId;
  msg: Message;
  sentAt: number;
  deliverAt: number;
  /** lost in the network: shown travelling part-way, then never delivered */
  dropped: boolean;
}

export interface TraceEntry {
  time: number;
  node: NodeId | null;
  kind: "election" | "leader" | "vote" | "commit" | "fault" | "client" | "log" | "violation";
  text: string;
}

export interface Stats {
  messagesSent: number;
  messagesDropped: number;
  elections: number;
  commits: number;
}

export class Simulator {
  readonly n: number;
  readonly ids: NodeId[];
  readonly raftConfig: RaftConfig;
  readonly network: NetworkConfig;
  readonly bugs: Bugs;
  readonly checker = new SafetyChecker();
  readonly trace: TraceEntry[] = [];
  readonly stats: Stats = { messagesSent: 0, messagesDropped: 0, elections: 0, commits: 0 };
  /** every action applied, with its time, so a run can be rebuilt (for rewinding) */
  readonly history: { time: number; action: Action }[] = [];

  time = 0;
  /** null while a server is crashed */
  nodes: (RaftNode | null)[];
  /** what each crashed server had on disk */
  private disks = new Map<NodeId, PersistentState>();
  /** links[a][b]: can a's messages reach b? */
  links: boolean[][];
  private flights: MinHeap<Flight>;
  private nextFlightId = 0;
  private netRng: Rng;
  private nodeRngs: Rng[];
  private eventsSinceLogCheck = 0;
  private logCheckEvery: number;
  private nextCommand = 1;

  constructor(opts: SimOptions = {}) {
    this.n = opts.nodes ?? 5;
    this.ids = Array.from({ length: this.n }, (_, i) => i + 1);
    this.raftConfig = { ...DEFAULT_CONFIG, ...opts.raft };
    this.network = { latencyMin: 10, latencyMax: 30, dropRate: 0, duplicateRate: 0, ...opts.network };
    this.bugs = opts.bugs ?? {};
    this.logCheckEvery = opts.logCheckEvery ?? 1;
    const root = new Rng(opts.seed ?? 1);
    this.netRng = root.fork();
    this.nodeRngs = this.ids.map(() => root.fork());
    this.links = this.ids.map(() => this.ids.map(() => true));
    this.flights = new MinHeap((a, b) => a.deliverAt < b.deliverAt || (a.deliverAt === b.deliverAt && a.id < b.id));
    this.nodes = this.ids.map((id) => this.makeNode(id));
  }

  private makeNode(id: NodeId, saved?: PersistentState): RaftNode {
    const peers = this.ids.filter((p) => p !== id);
    return new RaftNode(id, peers, this.raftConfig, this.nodeRngs[id - 1], this.bugs, this.time, saved);
  }

  node(id: NodeId): RaftNode | null { return this.nodes[id - 1]; }
  /** A server's log: its live log if it's up, or what's on its disk if it crashed. */
  logOf(id: NodeId): readonly { term: number; command: string | null }[] {
    return this.node(id)?.log ?? this.disks.get(id)?.log ?? [{ term: 0, command: null }];
  }
  isUp(id: NodeId): boolean { return this.nodes[id - 1] !== null; }
  inFlight(): readonly Flight[] { return this.flights.items; }

  /** The live leader with the highest term, if any. */
  leader(): RaftNode | null {
    let best: RaftNode | null = null;
    for (const node of this.nodes) {
      if (node && node.role === "leader" && (!best || node.currentTerm > best.currentTerm)) best = node;
    }
    return best;
  }

  /** The highest index committed by any server. */
  commitIndex(): number {
    return Math.max(0, ...this.nodes.map((n) => n?.commitIndex ?? 0));
  }

  // ------------------------------------------------------------ actions

  apply(action: Action): void {
    this.history.push({ time: this.time, action });
    const say = (text: string, node: NodeId | null = null, kind: TraceEntry["kind"] = "fault") =>
      this.trace.push({ time: this.time, node, kind, text });
    switch (action.kind) {
      case "crash": {
        const node = this.node(action.node);
        if (!node) return;
        this.disks.set(action.node, node.persistentState());
        this.nodes[action.node - 1] = null;
        say(`S${action.node} crashed`, action.node);
        break;
      }
      case "restart": {
        if (this.isUp(action.node)) return;
        const disk = this.disks.get(action.node);
        this.nodes[action.node - 1] = this.makeNode(action.node, disk);
        say(`S${action.node} restarted with term ${disk?.currentTerm ?? 0} and ${(disk?.log.length ?? 1) - 1} log entries from disk`, action.node);
        break;
      }
      case "partition": {
        const group = new Map<NodeId, number>();
        action.groups.forEach((g, i) => g.forEach((id) => group.set(id, i)));
        for (const a of this.ids) for (const b of this.ids) {
          this.links[a - 1][b - 1] = a === b || group.get(a) === group.get(b);
        }
        say(`Network partitioned into ${action.groups.map((g) => "{" + g.map((i) => "S" + i).join(", ") + "}").join(" and ")}`);
        break;
      }
      case "heal":
        for (const row of this.links) row.fill(true);
        say("Network healed: every server can reach every other");
        break;
      case "toggleLink": {
        const up = !this.links[action.a - 1][action.b - 1];
        this.links[action.a - 1][action.b - 1] = up;
        this.links[action.b - 1][action.a - 1] = up;
        say(`Link S${action.a}–S${action.b} ${up ? "restored" : "cut"}`);
        break;
      }
      case "setDropRate":
        this.network.dropRate = action.rate;
        say(`Message loss set to ${Math.round(action.rate * 100)}%`);
        break;
      case "submit": {
        const leader = action.node !== undefined ? this.node(action.node) : this.leader();
        if (!leader || leader.role !== "leader") {
          say(`Client request "${action.command}" failed: ${action.node !== undefined ? `S${action.node} isn't a leader` : "no leader right now"}`, null, "client");
          return;
        }
        const index = leader.submit(action.command, this.time);
        say(`Client sent "${action.command}" to leader S${leader.id}, which appended it at index ${index}`, leader.id, "client");
        this.drain(leader);
        break;
      }
    }
  }

  /** Submit an automatically numbered command. */
  submitNext(): void {
    this.apply({ kind: "submit", command: `x${this.nextCommand++}` });
  }

  // ------------------------------------------------------------ running

  /** Process every event up to and including time `t`. */
  runUntil(t: number): void {
    for (;;) {
      if (!this.checker.ok && this.stopOnViolation) return;
      const flight = this.flights.peek();
      let timerAt = Infinity, timerNode: RaftNode | null = null;
      for (const node of this.nodes) {
        if (node && node.nextDeadline() < timerAt) { timerAt = node.nextDeadline(); timerNode = node; }
      }
      const flightAt = flight ? flight.deliverAt : Infinity;
      // the clock never runs backwards: an overdue timer fires now, not in the past
      const next = Math.max(this.time, Math.min(flightAt, timerAt));
      if (next > t) break;
      this.time = next;
      if (flightAt <= timerAt) this.deliver(this.flights.pop()!);
      else { timerNode!.tick(this.time); this.drain(timerNode!); }
    }
    this.time = Math.max(this.time, t);
  }

  stopOnViolation = true;

  step(ms: number): void { this.runUntil(this.time + ms); }

  private deliver(f: Flight): void {
    if (f.dropped) return;
    const node = this.node(f.to);
    if (!node || !this.links[f.from - 1][f.to - 1]) {
      this.stats.messagesDropped++;
      return;
    }
    node.receive(f.from, f.msg, this.time);
    this.drain(node);
  }

  /** Send a node's outgoing messages into the network and record what happened inside it. */
  private drain(node: RaftNode): void {
    for (const env of node.takeMessages()) {
      this.stats.messagesSent++;
      const copies = this.netRng.chance(this.network.duplicateRate) ? 2 : 1;
      for (let c = 0; c < copies; c++) {
        const latency = this.netRng.int(this.network.latencyMin, this.network.latencyMax);
        const lost = this.netRng.chance(this.network.dropRate) || !this.links[env.from - 1][env.to - 1];
        if (lost) this.stats.messagesDropped++;
        this.flights.push({
          id: this.nextFlightId++, from: env.from, to: env.to, msg: env.msg,
          sentAt: this.time, deliverAt: this.time + latency, dropped: lost,
        });
      }
    }
    for (const ev of node.takeEvents()) this.record(node, ev);
    if (++this.eventsSinceLogCheck >= this.logCheckEvery) {
      this.eventsSinceLogCheck = 0;
      this.checkLogs();
    }
  }

  checkLogs(): void {
    const live: { id: NodeId; log: { term: number; command: string | null }[] }[] = [];
    this.ids.forEach((id) => {
      const node = this.node(id);
      const log = node ? node.log : this.disks.get(id)?.log;
      if (log) live.push({ id, log });
    });
    const before = this.checker.violations.length;
    this.checker.checkLogs(this.time, live);
    this.noteViolations(before);
  }

  private record(node: RaftNode, ev: NodeEvent): void {
    const before = this.checker.violations.length;
    this.checker.onEvent(this.time, node, ev);
    const say = (kind: TraceEntry["kind"], text: string) => this.trace.push({ time: this.time, node: node.id, kind, text });
    switch (ev.kind) {
      case "electionStarted":
        this.stats.elections++;
        say("election", `S${node.id} timed out and started an election for term ${ev.term}`);
        break;
      case "becameLeader":
        say("leader", `S${node.id} won the election for term ${ev.term} with ${ev.votes} of ${this.n} votes`);
        break;
      case "steppedDown":
        say("leader", `S${node.id} stepped down as leader: ${ev.reason}`);
        break;
      case "voted":
        say("vote", `S${node.id} voted for S${ev.candidate} in term ${ev.term}`);
        break;
      case "committed":
        if (node.role === "leader") {
          this.stats.commits++;
          say("commit", `S${node.id} committed index ${ev.index} ${ev.entry.command === null ? "(no-op)" : `"${ev.entry.command}"`}: it's on a majority of servers`);
        }
        break;
      case "truncated":
        say("log", `S${node.id} discarded ${ev.count} conflicting entr${ev.count === 1 ? "y" : "ies"} from index ${ev.fromIndex}`);
        break;
    }
    this.noteViolations(before);
  }

  private noteViolations(before: number): void {
    for (const v of this.checker.violations.slice(before)) {
      this.trace.push({ time: this.time, node: null, kind: "violation", text: `${v.property} violated: ${v.detail}` });
    }
  }
}
