import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { BUG_DESCRIPTIONS, type Bugs, RaftNode, Rng, DEFAULT_CONFIG, SafetyChecker, generateScenario, runMany, runScenario } from "../src/index.js";

describe("the chaos harness", () => {
  it("finds no safety violations in the correct implementation (1000 scenarios)", () => {
    const s = runMany(1, 1000);
    assert.equal(s.failures.length, 0, s.failures[0] && JSON.stringify(s.failures[0].violations[0]));
    assert.equal(s.livenessFailures.length, 0, `didn't recover: seeds ${s.livenessFailures.map((r) => r.seed)}`);
  });

  it("is reproducible: a seed always gives the same result", () => {
    assert.deepEqual(runScenario(77).stats, runScenario(77).stats);
  });

  it("generates scenarios with no engine-dependent randomness", () => {
    // Array.prototype.sort calls its comparator a different number of times in
    // different JavaScript engines, so a random comparator would make a seed
    // replay differently in a browser than in Node. Guard against it.
    const original = Array.prototype.sort;
    let randomComparatorUsed = false;
    Array.prototype.sort = function (this: unknown[], cmp?: (a: unknown, b: unknown) => number) {
      if (cmp && this.length > 1 && cmp(this[0], this[1]) !== cmp(this[0], this[1])) randomComparatorUsed = true;
      return original.call(this, cmp);
    } as typeof original;
    try { for (let seed = 1; seed <= 50; seed++) generateScenario(seed); }
    finally { Array.prototype.sort = original; }
    assert.equal(randomComparatorUsed, false);
  });

  // every deliberate bug must be caught; this proves the harness has teeth
  for (const bug of Object.keys(BUG_DESCRIPTIONS) as (keyof Bugs)[]) {
    it(`catches the injected bug: ${BUG_DESCRIPTIONS[bug]}`, () => {
      const s = runMany(1, 5000, { [bug]: true }, true);
      assert.ok(s.failures.length > 0, `${bug} survived 5000 scenarios`);
    });
  }
});

describe("the safety checker", () => {
  const node = (id: number) => new RaftNode(id, [1, 2, 3].filter((p) => p !== id), DEFAULT_CONFIG, new Rng(id));

  it("flags two leaders in one term", () => {
    const c = new SafetyChecker();
    c.onEvent(0, node(1), { kind: "becameLeader", term: 4, votes: 2 });
    c.onEvent(0, node(2), { kind: "becameLeader", term: 4, votes: 2 });
    assert.equal(c.violations[0]?.property, "Election Safety");
  });

  it("flags different entries applied at the same index", () => {
    const c = new SafetyChecker();
    c.onEvent(0, node(1), { kind: "applied", index: 3, entry: { term: 2, command: "a" } });
    c.onEvent(0, node(2), { kind: "applied", index: 3, entry: { term: 2, command: "b" } });
    assert.equal(c.violations[0]?.property, "State Machine Safety");
  });

  it("flags logs that agree at an index but differ earlier", () => {
    const c = new SafetyChecker();
    const sentinel = { term: 0, command: null };
    c.checkLogs(0, [
      { id: 1, log: [sentinel, { term: 1, command: "a" }, { term: 2, command: "c" }] },
      { id: 2, log: [sentinel, { term: 1, command: "b" }, { term: 2, command: "c" }] },
    ]);
    assert.equal(c.violations[0]?.property, "Log Matching");
  });

  it("flags a later leader missing a committed entry, but not an earlier-term leader", () => {
    const c = new SafetyChecker();
    const n1 = node(1);
    n1.currentTerm = 3;
    c.onEvent(0, n1, { kind: "committed", index: 1, entry: { term: 3, command: "x" } });
    c.onEvent(0, node(2), { kind: "becameLeader", term: 2, votes: 2 });
    assert.ok(c.ok, "a leader of an earlier term isn't bound by later commits");
    c.onEvent(0, node(3), { kind: "becameLeader", term: 4, votes: 2 });
    assert.equal(c.violations[0]?.property, "Leader Completeness");
  });
});
