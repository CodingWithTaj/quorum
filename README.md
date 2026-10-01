# Quorum

[![CI](https://github.com/CodingWithTaj/quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/CodingWithTaj/quorum/actions/workflows/ci.yml)

The Raft consensus algorithm, built from scratch in TypeScript: a deterministic cluster simulator, a chaos-testing harness that checks Raft's safety guarantees after every event, an interactive visualizer, and real servers that run the same code over HTTP.

**[Open the visualizer](https://CodingWithTaj.github.io/quorum/)**: watch elections and log replication, crash servers, cut network links, rewind time, and run the chaos lab in your browser.

## What's here

**A correct Raft implementation.** Leader election, log replication, persistence and the commit rules from the [Raft paper](https://raft.github.io/raft.pdf), with fast log backtracking and a no-op entry on election. `RaftNode` is a pure state machine: it never reads a clock or touches the network. The caller passes in the time and incoming messages and collects outgoing ones, so the same code runs in the simulator, the tests and real servers.

**A deterministic simulator.** Virtual time, a simulated network with configurable latency, loss, duplication and reordering, partitions, and crashes that keep only what a real server writes to disk. Every source of randomness comes from one seed, so any run can be replayed exactly.

**A safety checker.** After every event, it verifies the four guarantees from Figure 3 of the paper:

| Property | Meaning |
|---|---|
| Election Safety | At most one leader is elected per term |
| Log Matching | Logs that agree on an entry's index and term are identical up to it |
| Leader Completeness | A committed entry appears in the log of every leader of a later term |
| State Machine Safety | No two servers ever apply different commands at the same index |

**A chaos harness.** Thousands of randomized scenarios: crashes, instant crash-and-restart "bounces", partitions, single cut links, message loss and duplication, and varied election timing, all while clients keep writing. After the chaos, the network heals and the harness checks the cluster recovers.

**Proof that the harness works.** Four realistic Raft bugs can be switched on, and the harness catches every one:

| Injected bug | Caught at | What breaks |
|---|---|---|
| Vote for a candidate without checking its log is up to date | seed 1 | Leader Completeness |
| Accept log entries from a leader with a stale term | seed 1 | Leader Completeness |
| Commit earlier-term entries by counting replicas (Figure 8) | seed 81 | Leader Completeness |
| Forget your vote when you restart | seed 511 | Election Safety: two leaders in one term |

**Real servers.** `server/node.ts` runs `RaftNode` with a real clock, HTTP between servers, and state saved to disk with atomic writes before any message is sent. `npm run cluster` starts five servers, writes data, kills the leader with `SIGKILL`, keeps writing, restarts it, and checks every server ends with the identical log. There's also a Docker Compose file for running each server in its own container.

## Results

```
$ npm run chaos -- --runs 10000
Ran 10000 scenarios (seeds 1–10000) in 12.1s
  2097.9 minutes of simulated cluster time, 22,837,312 messages

Safety: all four Raft guarantees held in every scenario.
```

## What the harness found during development

The chaos harness earned its place before the visualizer existed. Its first run against the "finished" implementation reported a Leader Completeness violation on seed 12. Replaying the seed showed two bugs at once:

1. **In the Raft code:** a leader that stepped down after seeing a higher term kept its election deadline from the last time it was a follower, long in the past, so it immediately started a disruptive election. Leaders don't run an election timer, so stepping down now starts a fresh one.
2. **In the checker:** it demanded committed entries in *every* later leader, but the guarantee only covers leaders of *later terms*. A leader elected in an earlier term isn't bound by it.

The browser exposed a subtler flaw in the harness itself. The same seed caught the Figure 8 bug from the command line but not in Chrome. The scenario generator shuffled servers with `array.sort(() => rng.next() - 0.5)`, and how many times `sort` calls its comparator depends on the JavaScript engine, so Node and Chrome consumed different numbers of random values and built *different scenarios from the same seed*. That breaks the one promise a deterministic simulator makes. The fix is a Fisher–Yates shuffle, which uses exactly one random number per element on every engine, plus a test that fails if any scenario code ever sorts with a random comparator again.

It also demonstrated Raft's **timing requirement**. With election timeouts spread over only 10 ms and network delays up to 54 ms, one scenario went through 33 consecutive split votes before electing a leader. Safety held throughout, as Raft promises, but liveness depends on timeouts being spread more widely than message delays. The harness now checks liveness only for scenarios that meet that requirement, while keeping the tight-timing scenarios because they're the best at provoking safety bugs.

## Running it

You need Node.js 22.

```bash
npm install
npm test                      # unit, scenario and safety tests
npm run chaos                 # 1,000 chaos scenarios
npm run chaos -- --runs 10000 --seed 500
npm run chaos -- --bug commitOldTerms   # watch it catch an injected bug
npm run cluster               # a real 5-process cluster over HTTP
npm run site                  # build the visualizer into site/
```

Open `site/index.html` in a browser after `npm run site`, or `site/quorum.html`, a single self-contained file.

With Docker:

```bash
docker compose up --build
curl -X POST localhost:7001/submit -H "content-type: application/json" -d '{"command":"set x=1"}'
curl localhost:7001/status
docker compose kill s1     # crash a server
docker compose start s1    # it reloads its state and catches up
```

A server that isn't the leader answers `/submit` with status 421 and the leader's id, so clients can retry there.

## Layout

| Path | What it is |
|---|---|
| `src/raft.ts` | The Raft server as a pure state machine, commented with paper section numbers |
| `src/sim.ts` | Deterministic cluster simulator |
| `src/invariants.ts` | The safety checker |
| `src/chaos.ts` | Scenario generator and chaos runner |
| `src/types.ts` | Messages, configuration and the injectable bugs |
| `test/` | Tests for elections, replication, persistence, partitions, determinism, the checker, and bug detection |
| `cli/chaos.ts` | Command-line chaos runner |
| `server/` | Real HTTP server and the cluster demo |
| `web/` | The visualizer |

## References

- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf), 2014
- Diego Ongaro, [Consensus: Bridging Theory and Practice](https://github.com/ongardie/dissertation), 2014 (the no-op on election, §6.4)

## License

MIT
