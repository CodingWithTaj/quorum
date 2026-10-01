import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { DEFAULT_CONFIG, RaftNode, Rng, Simulator, type RequestVote } from "../src/index.js";

const leaders = (sim: Simulator) => sim.nodes.filter((n) => n?.role === "leader");

describe("elections", () => {
  it("elects exactly one leader", () => {
    const sim = new Simulator({ seed: 7 });
    sim.runUntil(1000);
    assert.equal(leaders(sim).length, 1);
    assert.ok(sim.checker.ok);
  });

  it("elects a new leader after the leader crashes", () => {
    const sim = new Simulator({ seed: 3 });
    sim.runUntil(1000);
    const first = sim.leader()!;
    sim.apply({ kind: "crash", node: first.id });
    sim.runUntil(2000);
    const second = sim.leader()!;
    assert.ok(second && second.id !== first.id);
    assert.ok(second.currentTerm > first.currentTerm);
  });

  it("refuses to vote for a candidate with a less up-to-date log (§5.4.1)", () => {
    const node = new RaftNode(1, [2, 3], DEFAULT_CONFIG, new Rng(1));
    node.currentTerm = 2;
    node.log.push({ term: 2, command: "a" });
    const ask = (lastLogTerm: number, lastLogIndex: number): boolean => {
      const n = new RaftNode(1, [2, 3], DEFAULT_CONFIG, new Rng(1), {}, 0, node.persistentState());
      const vote: RequestVote = { type: "RequestVote", term: 3, candidateId: 2, lastLogIndex, lastLogTerm };
      n.receive(2, vote, 0);
      const reply = n.takeMessages()[0].msg;
      return reply.type === "RequestVoteReply" && reply.voteGranted;
    };
    assert.equal(ask(1, 5), false, "older last term loses even with a longer log");
    assert.equal(ask(2, 0), false, "same last term but shorter log loses");
    assert.equal(ask(2, 1), true);
    assert.equal(ask(3, 1), true);
  });

  it("votes at most once per term, and remembers its vote across a restart", () => {
    const node = new RaftNode(1, [2, 3], DEFAULT_CONFIG, new Rng(1));
    const vote = (candidateId: number): boolean => {
      node.receive(candidateId, { type: "RequestVote", term: 1, candidateId, lastLogIndex: 0, lastLogTerm: 0 }, 0);
      const m = node.takeMessages()[0].msg;
      return m.type === "RequestVoteReply" && m.voteGranted;
    };
    assert.equal(vote(2), true);
    assert.equal(vote(3), false);
    const restarted = new RaftNode(1, [2, 3], DEFAULT_CONFIG, new Rng(1), {}, 0, node.persistentState());
    restarted.receive(3, { type: "RequestVote", term: 1, candidateId: 3, lastLogIndex: 0, lastLogTerm: 0 }, 0);
    const m = restarted.takeMessages()[0].msg;
    assert.ok(m.type === "RequestVoteReply" && !m.voteGranted);
  });
});

describe("log replication", () => {
  it("replicates and applies commands on every server in the same order", () => {
    const sim = new Simulator({ seed: 11 });
    sim.runUntil(1000);
    for (let i = 0; i < 20; i++) { sim.submitNext(); sim.step(20); }
    sim.step(500);
    const applied = sim.nodes.map((n) => n!.applied.filter((c) => c !== null));
    for (const a of applied) assert.deepEqual(a, applied[0]);
    assert.equal(applied[0].length, 20);
    assert.ok(sim.checker.ok);
  });

  it("a leader cut off in a minority can't commit, and its entries are replaced after healing", () => {
    const sim = new Simulator({ seed: 5 });
    sim.runUntil(1000);
    const old = sim.leader()!;
    const others = sim.ids.filter((i) => i !== old.id);
    sim.apply({ kind: "partition", groups: [[old.id, others[0]], others.slice(1)] });
    sim.apply({ kind: "submit", command: "lost" });
    const stuck = old.commitIndex;
    sim.step(1500);
    assert.equal(old.commitIndex, stuck, "the minority leader must not commit");
    const majorityLeader = sim.leader()!;
    assert.notEqual(majorityLeader.id, old.id);
    sim.apply({ kind: "submit", command: "kept" });
    sim.step(300);
    sim.apply({ kind: "heal" });
    sim.step(1500);
    assert.notEqual(old.role, "leader");
    for (const n of sim.nodes) {
      assert.ok(n!.applied.includes("kept"));
      assert.ok(!n!.applied.includes("lost"), `S${n!.id} applied an uncommitted entry`);
    }
    assert.ok(sim.trace.some((e) => e.text.includes("discarded")));
    assert.ok(sim.checker.ok);
  });

  it("a restarted server keeps its term and log, and catches up", () => {
    const sim = new Simulator({ seed: 9 });
    sim.runUntil(1000);
    for (let i = 0; i < 5; i++) sim.submitNext();
    sim.step(300);
    const victim = sim.ids.find((id) => id !== sim.leader()!.id)!;
    const before = sim.node(victim)!.persistentState();
    sim.apply({ kind: "crash", node: victim });
    for (let i = 0; i < 5; i++) sim.submitNext();
    sim.step(300);
    sim.apply({ kind: "restart", node: victim });
    const restarted = sim.node(victim)!;
    assert.equal(restarted.currentTerm, before.currentTerm);
    assert.equal(restarted.log.length, before.log.length);
    sim.step(1000);
    assert.equal(restarted.applied.filter((c) => c !== null).length, 10);
  });
});

describe("determinism", () => {
  it("the same seed and actions produce the identical run", () => {
    const run = () => {
      const sim = new Simulator({ seed: 42, network: { dropRate: 0.1, duplicateRate: 0.05 } });
      sim.runUntil(800);
      sim.submitNext();
      sim.apply({ kind: "crash", node: 2 });
      sim.runUntil(2500);
      return JSON.stringify(sim.trace) + JSON.stringify(sim.nodes.map((n) => n?.log));
    };
    assert.equal(run(), run());
  });
});
