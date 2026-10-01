#!/usr/bin/env node
/**
 * Command-line chaos runner.
 *
 *   npm run chaos                         # 1000 scenarios against the real implementation
 *   npm run chaos -- --runs 5000 --seed 42
 *   npm run chaos -- --bug commitOldTerms # prove the harness catches a deliberate bug
 */
import { BUG_DESCRIPTIONS, type Bugs, PROPERTY_DESCRIPTIONS, runMany } from "../src/index.js";

const args = process.argv.slice(2);
const get = (flag: string, fallback: string) => {
  const i = args.indexOf(flag);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};
const runs = Number(get("--runs", "1000"));
const seed = Number(get("--seed", "1"));
const bugName = get("--bug", "");
const bugs: Bugs = {};
if (bugName) {
  if (!(bugName in BUG_DESCRIPTIONS)) {
    console.error(`unknown bug "${bugName}". Choose from: ${Object.keys(BUG_DESCRIPTIONS).join(", ")}`);
    process.exit(2);
  }
  bugs[bugName as keyof Bugs] = true;
  console.log(`Injected bug: ${BUG_DESCRIPTIONS[bugName as keyof Bugs]}\n`);
}

const started = Date.now();
const s = runMany(seed, runs, bugs, !!bugName);
const secs = (Date.now() - started) / 1000;

console.log(`Ran ${s.runs} scenarios (seeds ${seed}–${seed + s.runs - 1}) in ${secs.toFixed(1)}s`);
console.log(`  ${(s.totalSimulatedMs / 1000 / 60).toFixed(1)} minutes of simulated cluster time, ${s.totalMessages.toLocaleString()} messages\n`);

if (s.failures.length === 0) {
  console.log("Safety: all four Raft guarantees held in every scenario.");
  for (const [p, d] of Object.entries(PROPERTY_DESCRIPTIONS)) console.log(`  ✓ ${p}: ${d}`);
} else {
  const f = s.failures[0];
  const v = f.violations[0];
  console.log(`Safety violation found with seed ${f.seed} at t=${v.time}ms`);
  console.log(`  ${v.property}: ${v.detail}`);
  console.log(`\nReplay it in the visualizer's Chaos lab with seed ${f.seed}.`);
}
if (s.livenessFailures.length) {
  console.log(`\nLiveness: ${s.livenessFailures.length} scenario(s) didn't recover in time (seeds ${s.livenessFailures.slice(0, 5).map((r) => r.seed).join(", ")}).`);
}
process.exit(bugName ? (s.failures.length ? 0 : 1) : (s.failures.length || s.livenessFailures.length ? 1 : 0));
