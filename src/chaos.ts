/**
 * Chaos testing: run many randomised disaster scenarios and check Raft's
 * safety guarantees throughout each one.
 *
 * A scenario is generated entirely from its seed, so a failing seed can be
 * replayed exactly, in the test runner or in the browser visualizer.
 */
import type { Violation } from "./invariants.js";
import { Rng } from "./rng.js";
import { type Action, Simulator } from "./sim.js";
import type { Bugs, NodeId } from "./types.js";

export interface ScenarioStep { time: number; action: Action }

export interface Scenario {
  seed: number;
  duration: number;
  network: { latencyMin: number; latencyMax: number; dropRate: number; duplicateRate: number };
  raft: { electionTimeoutMin: number; electionTimeoutMax: number; heartbeatInterval: number; maxBatch: number };
  steps: ScenarioStep[];
}

export interface ChaosResult {
  seed: number;
  violations: Violation[];
  /** after the faults stopped and the network healed, did the cluster recover? */
  recovered: boolean;
  /**
   * Raft only promises liveness when election timeouts are spread out more than
   * messages take to arrive; otherwise candidates keep splitting the vote.
   * Recovery is only checked for scenarios that meet this timing requirement.
   */
  livenessChecked: boolean;
  stats: Simulator["stats"];
  simulatedMs: number;
}

/** Turn a seed into a schedule of crashes, partitions, message loss and client requests. */
export function generateScenario(seed: number, duration = 10_000): Scenario {
  const rng = new Rng(seed);
  const ids: NodeId[] = [1, 2, 3, 4, 5];
  const network = {
    latencyMin: rng.int(1, 10),
    latencyMax: rng.int(15, 60),
    dropRate: rng.pick([0, 0, 0.02, 0.05, 0.1, 0.2]),
    duplicateRate: rng.pick([0, 0, 0.02, 0.05]),
  };
  // narrow timeout ranges cause simultaneous candidates and split votes;
  // small batches make old entries replicate separately from new ones
  const raft = {
    electionTimeoutMin: 150,
    electionTimeoutMax: rng.pick([160, 175, 200, 300]),
    heartbeatInterval: 50,
    maxBatch: rng.pick([1, 2, 4, 16]),
  };
  const steps: ScenarioStep[] = [];
  const down = new Set<NodeId>();
  let t = 0;
  let cmd = 1;
  while (t < duration) {
    t += rng.int(20, 200);
    const r = rng.next();
    let action: Action;
    if (r < 0.55) action = { kind: "submit", command: `x${cmd++}` };
    else if (r < 0.67 && down.size < 3) {
      const node = rng.pick(ids.filter((i) => !down.has(i)));
      down.add(node);
      action = { kind: "crash", node };
    } else if (r < 0.79 && down.size > 0) {
      const node = rng.pick([...down]);
      down.delete(node);
      action = { kind: "restart", node };
    } else if (r < 0.87) {
      const shuffled = rng.shuffle(ids);
      const cut = rng.int(1, 4);
      action = { kind: "partition", groups: [shuffled.slice(0, cut), shuffled.slice(cut)] };
    } else if (r < 0.94) action = { kind: "heal" };
    else action = { kind: "toggleLink", a: rng.pick(ids), b: rng.pick(ids) };
    if (action.kind === "toggleLink" && action.a === action.b) continue;
    steps.push({ time: t, action });
    // sometimes a crash is a quick "bounce": the server is back within milliseconds,
    // possibly in the middle of the same election
    if (action.kind === "crash" && rng.chance(0.75)) {
      steps.push({ time: t + rng.int(1, 40), action: { kind: "restart", node: action.node } });
      down.delete(action.node);
    }
  }
  steps.sort((a, b) => a.time - b.time);
  return { seed, duration, network, raft, steps };
}

export function buildSimulator(scenario: Scenario, bugs: Bugs = {}, logCheckEvery = 25): Simulator {
  return new Simulator({ seed: scenario.seed, network: scenario.network, raft: scenario.raft, bugs, logCheckEvery });
}

/** Run one scenario, then heal everything and check the cluster recovers. */
export function runScenario(seed: number, bugs: Bugs = {}, duration = 10_000): ChaosResult {
  const scenario = generateScenario(seed, duration);
  const sim = buildSimulator(scenario, bugs);
  for (const step of scenario.steps) {
    sim.runUntil(step.time);
    if (!sim.checker.ok) break;
    sim.apply(step.action);
  }
  let recovered = true;
  const livenessChecked =
    scenario.raft.electionTimeoutMax - scenario.raft.electionTimeoutMin >= scenario.network.latencyMax;
  if (sim.checker.ok && livenessChecked) {
    sim.runUntil(scenario.duration);
    // liveness: with every server up and a reliable network, a leader must emerge and commit
    sim.apply({ kind: "heal" });
    sim.apply({ kind: "setDropRate", rate: 0 });
    for (const id of sim.ids) sim.apply({ kind: "restart", node: id });
    sim.runUntil(sim.time + 3_000);
    const before = sim.commitIndex();
    sim.apply({ kind: "submit", command: "final" });
    sim.runUntil(sim.time + 2_000);
    recovered = sim.leader() !== null && sim.commitIndex() > before;
  }
  sim.checkLogs();
  return { seed, violations: sim.checker.violations, recovered, livenessChecked, stats: sim.stats, simulatedMs: sim.time };
}

export interface ChaosSummary {
  runs: number;
  failures: ChaosResult[];
  livenessFailures: ChaosResult[];
  totalMessages: number;
  totalSimulatedMs: number;
}

export function runMany(firstSeed: number, runs: number, bugs: Bugs = {}, stopAtFirst = false): ChaosSummary {
  const summary: ChaosSummary = { runs: 0, failures: [], livenessFailures: [], totalMessages: 0, totalSimulatedMs: 0 };
  for (let i = 0; i < runs; i++) {
    const r = runScenario(firstSeed + i, bugs);
    summary.runs++;
    summary.totalMessages += r.stats.messagesSent;
    summary.totalSimulatedMs += r.simulatedMs;
    if (r.violations.length) {
      summary.failures.push(r);
      if (stopAtFirst) break;
    } else if (!r.recovered) summary.livenessFailures.push(r);
  }
  return summary;
}
