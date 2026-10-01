/**
 * One Raft server, written as a pure state machine.
 *
 * It never reads a clock, opens a socket or sets a timer. The caller tells
 * it the time, hands it incoming messages, and collects the messages it
 * wants to send. That makes the same code usable in a deterministic
 * simulator, in tests, and in a real networked server.
 *
 * Section numbers (§5.2 and so on) refer to the Raft paper:
 * "In Search of an Understandable Consensus Algorithm", Ongaro & Ousterhout, 2014.
 */
import type { Rng } from "./rng.js";
import type {
  AppendEntries, AppendEntriesReply, Bugs, Envelope, LogEntry, Message, NodeId,
  PersistentState, RaftConfig, RequestVote, RequestVoteReply, Role,
} from "./types.js";

/** Things that happened inside a node, for narration and safety checking. */
export type NodeEvent =
  | { kind: "electionStarted"; term: number }
  | { kind: "becameLeader"; term: number; votes: number }
  | { kind: "steppedDown"; term: number; reason: string }
  | { kind: "voted"; term: number; candidate: NodeId }
  | { kind: "committed"; index: number; entry: LogEntry }
  | { kind: "applied"; index: number; entry: LogEntry }
  | { kind: "truncated"; fromIndex: number; count: number };

export class RaftNode {
  // persistent state (§5, Figure 2): survives a crash
  currentTerm = 0;
  votedFor: NodeId | null = null;
  /** log[0] is a sentinel so real entries start at index 1, as in the paper */
  log: LogEntry[] = [{ term: 0, command: null }];

  // volatile state
  role: Role = "follower";
  leaderId: NodeId | null = null;
  commitIndex = 0;
  lastApplied = 0;
  /** commands applied to the state machine, in order */
  applied: (string | null)[] = [];

  // candidate state
  votes = new Set<NodeId>();

  // leader state, reinitialised after each election
  nextIndex = new Map<NodeId, number>();
  matchIndex = new Map<NodeId, number>();

  // timers, as absolute times
  electionDeadline = 0;
  electionTimeout = 0;
  heartbeatDue = 0;

  private outbox: Envelope[] = [];
  private events: NodeEvent[] = [];

  constructor(
    readonly id: NodeId,
    readonly peers: NodeId[],
    readonly config: RaftConfig,
    private rng: Rng,
    private bugs: Bugs = {},
    now = 0,
    saved?: PersistentState,
  ) {
    if (saved) {
      this.currentTerm = saved.currentTerm;
      this.votedFor = bugs.forgetVoteOnRestart ? null : saved.votedFor;
      this.log = saved.log.map((e) => ({ ...e }));
    }
    this.resetElectionTimer(now);
  }

  // ---------------------------------------------------------------- inputs

  /** Advance to time `now`, firing any timers that are due. */
  tick(now: number): void {
    if (this.role === "leader") {
      if (now >= this.heartbeatDue) this.broadcastAppendEntries(now);
    } else if (now >= this.electionDeadline) {
      this.startElection(now);
    }
  }

  receive(from: NodeId, msg: Message, now: number): void {
    // §5.1: any message from a later term means we're out of date
    if (msg.term > this.currentTerm) {
      const wasLeader = this.role === "leader";
      this.currentTerm = msg.term;
      this.votedFor = null;
      // A follower or candidate keeps its running election timer here, but a
      // leader has none: it needs a fresh one, or it would time out at once.
      this.becomeFollower(now, wasLeader);
      if (wasLeader) this.events.push({ kind: "steppedDown", term: msg.term, reason: `saw term ${msg.term} from S${from}` });
    }
    switch (msg.type) {
      case "RequestVote": return this.onRequestVote(from, msg, now);
      case "RequestVoteReply": return this.onRequestVoteReply(from, msg, now);
      case "AppendEntries": return this.onAppendEntries(from, msg, now);
      case "AppendEntriesReply": return this.onAppendEntriesReply(from, msg, now);
    }
  }

  /** Propose a client command. Returns its log index, or null if this node isn't leader. */
  submit(command: string, now: number): number | null {
    if (this.role !== "leader") return null;
    this.log.push({ term: this.currentTerm, command });
    this.matchIndex.set(this.id, this.lastLogIndex());
    this.broadcastAppendEntries(now);
    this.advanceCommitIndex();
    return this.lastLogIndex();
  }

  // ---------------------------------------------------------------- outputs

  takeMessages(): Envelope[] {
    const out = this.outbox;
    this.outbox = [];
    return out;
  }

  takeEvents(): NodeEvent[] {
    const out = this.events;
    this.events = [];
    return out;
  }

  persistentState(): PersistentState {
    return { currentTerm: this.currentTerm, votedFor: this.votedFor, log: this.log.map((e) => ({ ...e })) };
  }

  /** The earliest time this node's timers need attention. */
  nextDeadline(): number {
    return this.role === "leader" ? this.heartbeatDue : this.electionDeadline;
  }

  lastLogIndex(): number { return this.log.length - 1; }
  lastLogTerm(): number { return this.log[this.log.length - 1].term; }
  majority(): number { return Math.floor((this.peers.length + 1) / 2) + 1; }

  // ---------------------------------------------------------------- elections (§5.2)

  private resetElectionTimer(now: number): void {
    // randomised timeouts make split votes rare
    this.electionTimeout = this.rng.int(this.config.electionTimeoutMin, this.config.electionTimeoutMax);
    this.electionDeadline = now + this.electionTimeout;
  }

  private becomeFollower(now: number, resetTimer = true): void {
    this.role = "follower";
    this.votes.clear();
    if (resetTimer) this.resetElectionTimer(now);
  }

  private startElection(now: number): void {
    this.role = "candidate";
    this.currentTerm += 1;
    this.votedFor = this.id;
    this.leaderId = null;
    this.votes = new Set([this.id]);
    this.resetElectionTimer(now);
    this.events.push({ kind: "electionStarted", term: this.currentTerm });
    for (const peer of this.peers) {
      this.send(peer, {
        type: "RequestVote",
        term: this.currentTerm,
        candidateId: this.id,
        lastLogIndex: this.lastLogIndex(),
        lastLogTerm: this.lastLogTerm(),
      });
    }
    if (this.votes.size >= this.majority()) this.becomeLeader(now); // a one-node cluster
  }

  private onRequestVote(from: NodeId, msg: RequestVote, now: number): void {
    // §5.4.1: only vote for candidates whose log is at least as up to date as ours
    const upToDate =
      msg.lastLogTerm > this.lastLogTerm() ||
      (msg.lastLogTerm === this.lastLogTerm() && msg.lastLogIndex >= this.lastLogIndex());
    const grant =
      msg.term === this.currentTerm &&
      (this.votedFor === null || this.votedFor === msg.candidateId) &&
      (upToDate || !!this.bugs.voteWithoutLogCheck);
    if (grant) {
      this.votedFor = msg.candidateId;
      this.resetElectionTimer(now); // granting a vote counts as hearing from a would-be leader
      this.events.push({ kind: "voted", term: this.currentTerm, candidate: msg.candidateId });
    }
    this.send(from, { type: "RequestVoteReply", term: this.currentTerm, voteGranted: grant });
  }

  private onRequestVoteReply(from: NodeId, msg: RequestVoteReply, now: number): void {
    if (this.role !== "candidate" || msg.term !== this.currentTerm || !msg.voteGranted) return;
    this.votes.add(from);
    if (this.votes.size >= this.majority()) this.becomeLeader(now);
  }

  private becomeLeader(now: number): void {
    this.role = "leader";
    this.leaderId = this.id;
    this.events.push({ kind: "becameLeader", term: this.currentTerm, votes: this.votes.size });
    // a no-op entry from the new term lets entries from earlier terms commit (§5.4.2, §8)
    if (!this.bugs.commitOldTerms) this.log.push({ term: this.currentTerm, command: null });
    this.nextIndex.clear();
    this.matchIndex.clear();
    for (const peer of this.peers) {
      // start by sending our last entry (the no-op), so followers receive it in the first round;
      // a failed consistency check walks nextIndex back from there (§5.3)
      this.nextIndex.set(peer, Math.max(1, this.lastLogIndex()));
      this.matchIndex.set(peer, 0);
    }
    this.matchIndex.set(this.id, this.lastLogIndex());
    this.broadcastAppendEntries(now);
    this.advanceCommitIndex();
  }

  // ---------------------------------------------------------------- log replication (§5.3)

  private broadcastAppendEntries(now: number): void {
    for (const peer of this.peers) this.sendAppendEntries(peer);
    this.heartbeatDue = now + this.config.heartbeatInterval;
  }

  private sendAppendEntries(peer: NodeId): void {
    const next = this.nextIndex.get(peer) ?? this.lastLogIndex() + 1;
    const prevLogIndex = next - 1;
    this.send(peer, {
      type: "AppendEntries",
      term: this.currentTerm,
      leaderId: this.id,
      prevLogIndex,
      prevLogTerm: this.log[prevLogIndex].term,
      entries: this.log.slice(next, next + this.config.maxBatch).map((e) => ({ ...e })),
      leaderCommit: this.commitIndex,
    });
  }

  private onAppendEntries(from: NodeId, msg: AppendEntries, now: number): void {
    const reply = (success: boolean, matchIndex: number, conflictIndex: number) =>
      this.send(from, { type: "AppendEntriesReply", term: this.currentTerm, success, matchIndex, conflictIndex });

    if (msg.term < this.currentTerm && !this.bugs.acceptStaleLeader) {
      return reply(false, 0, 0); // a leader from an old term: tell it the new term
    }
    // a current leader exists, so stop any election of our own
    if (this.role !== "follower") this.becomeFollower(now);
    else this.resetElectionTimer(now);
    this.leaderId = msg.leaderId;

    // consistency check: our log must contain the entry just before the new ones
    if (msg.prevLogIndex > this.lastLogIndex()) {
      return reply(false, 0, this.lastLogIndex() + 1);
    }
    if (this.log[msg.prevLogIndex].term !== msg.prevLogTerm) {
      // skip back past the whole conflicting term in one step
      const badTerm = this.log[msg.prevLogIndex].term;
      let i = msg.prevLogIndex;
      while (i > 1 && this.log[i - 1].term === badTerm) i--;
      return reply(false, 0, i);
    }

    // append, deleting any conflicting suffix (never delete entries that match)
    let index = msg.prevLogIndex;
    for (const entry of msg.entries) {
      index++;
      if (index <= this.lastLogIndex()) {
        if (this.log[index].term === entry.term) continue;
        const removed = this.log.length - index;
        this.log.length = index;
        this.events.push({ kind: "truncated", fromIndex: index, count: removed });
      }
      this.log.push({ ...entry });
    }
    const lastNew = msg.prevLogIndex + msg.entries.length;
    if (msg.leaderCommit > this.commitIndex) {
      this.setCommitIndex(Math.min(msg.leaderCommit, lastNew));
    }
    reply(true, lastNew, 0);
  }

  private onAppendEntriesReply(from: NodeId, msg: AppendEntriesReply, _now: number): void {
    if (this.role !== "leader" || msg.term !== this.currentTerm) return; // stale reply
    if (msg.success) {
      if (msg.matchIndex > (this.matchIndex.get(from) ?? 0)) this.matchIndex.set(from, msg.matchIndex);
      this.nextIndex.set(from, (this.matchIndex.get(from) ?? 0) + 1);
      this.advanceCommitIndex();
      // keep streaming if the follower is still behind
      if ((this.nextIndex.get(from) ?? 0) <= this.lastLogIndex()) this.sendAppendEntries(from);
    } else if (msg.conflictIndex > 0) {
      const current = this.nextIndex.get(from) ?? 1;
      this.nextIndex.set(from, Math.max(1, Math.min(current - 1, msg.conflictIndex)));
      this.sendAppendEntries(from);
    }
  }

  /** §5.3, §5.4.2: commit the highest index stored on a majority, if it's from the current term. */
  private advanceCommitIndex(): void {
    for (let n = this.lastLogIndex(); n > this.commitIndex; n--) {
      if (this.log[n].term !== this.currentTerm && !this.bugs.commitOldTerms) break;
      let count = 0;
      for (const [, m] of this.matchIndex) if (m >= n) count++;
      if (count >= this.majority()) {
        this.setCommitIndex(n);
        break;
      }
    }
  }

  private setCommitIndex(n: number): void {
    if (n <= this.commitIndex) return;
    for (let i = this.commitIndex + 1; i <= n; i++) {
      this.events.push({ kind: "committed", index: i, entry: this.log[i] });
    }
    this.commitIndex = n;
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      const entry = this.log[this.lastApplied];
      this.applied.push(entry.command);
      this.events.push({ kind: "applied", index: this.lastApplied, entry });
    }
  }

  private send(to: NodeId, msg: Message): void {
    this.outbox.push({ from: this.id, to, msg });
  }
}
