/**
 * Checks Raft's safety guarantees (Figure 3 of the Raft paper) as the
 * cluster runs. If any of these is ever false, the implementation is wrong.
 */
import type { NodeEvent, RaftNode } from "./raft.js";
import type { LogEntry, NodeId } from "./types.js";

export type Property =
  | "Election Safety"
  | "Log Matching"
  | "Leader Completeness"
  | "State Machine Safety";

export const PROPERTY_DESCRIPTIONS: Record<Property, string> = {
  "Election Safety": "At most one leader can be elected in a given term.",
  "Log Matching": "If two logs contain an entry with the same index and term, the logs are identical up to that index.",
  "Leader Completeness": "If an entry is committed in a term, it is present in the logs of the leaders of all later terms.",
  "State Machine Safety": "If a server has applied an entry at an index, no other server ever applies a different entry at that index.",
};

export interface Violation {
  property: Property;
  time: number;
  detail: string;
}

const same = (a: LogEntry, b: LogEntry) => a.term === b.term && a.command === b.command;
const show = (e: LogEntry) => `(term ${e.term}, ${e.command === null ? "no-op" : JSON.stringify(e.command)})`;

export class SafetyChecker {
  readonly violations: Violation[] = [];
  private leaders = new Map<number, NodeId>();
  /** every entry known to be committed, by index, with the earliest term it was known committed in */
  private committed = new Map<number, { entry: LogEntry; term: number }>();
  /** the entry applied at each index by the first server to apply it */
  private applied = new Map<number, { node: NodeId; entry: LogEntry }>();

  onEvent(time: number, node: RaftNode, ev: NodeEvent): void {
    switch (ev.kind) {
      case "becameLeader": {
        const other = this.leaders.get(ev.term);
        if (other !== undefined && other !== node.id) {
          this.fail("Election Safety", time, `S${other} and S${node.id} were both elected leader in term ${ev.term}`);
        }
        this.leaders.set(ev.term, node.id);
        for (const [index, { entry, term }] of this.committed) {
          // the guarantee covers leaders of *later* terms only
          if (term >= ev.term) continue;
          const mine = node.log[index];
          if (!mine || !same(mine, entry)) {
            this.fail("Leader Completeness", time,
              `S${node.id} became leader for term ${ev.term} without committed entry ${index} ${show(entry)}; it has ${mine ? show(mine) : "nothing"} there`);
            break;
          }
        }
        break;
      }
      case "committed": {
        const known = this.committed.get(ev.index);
        if (!known) this.committed.set(ev.index, { entry: { ...ev.entry }, term: node.currentTerm });
        else if (node.currentTerm < known.term) known.term = node.currentTerm;
        break;
      }
      case "applied": {
        const first = this.applied.get(ev.index);
        if (!first) this.applied.set(ev.index, { node: node.id, entry: { ...ev.entry } });
        else if (!same(first.entry, ev.entry)) {
          this.fail("State Machine Safety", time,
            `S${first.node} applied ${show(first.entry)} at index ${ev.index}, but S${node.id} applied ${show(ev.entry)}`);
        }
        break;
      }
    }
  }

  /** Log Matching across every pair of servers. */
  checkLogs(time: number, nodes: { id: NodeId; log: LogEntry[] }[]): void {
    for (let a = 0; a < nodes.length; a++) {
      for (let b = a + 1; b < nodes.length; b++) {
        const x = nodes[a].log, y = nodes[b].log;
        // find the last index where both logs have an entry with the same term...
        let i = Math.min(x.length, y.length) - 1;
        while (i > 0 && x[i].term !== y[i].term) i--;
        // ...then everything up to there must be identical
        for (let j = i; j > 0; j--) {
          if (!same(x[j], y[j])) {
            this.fail("Log Matching", time,
              `S${nodes[a].id} and S${nodes[b].id} agree at index ${i} (term ${x[i].term}) but differ at index ${j}: ${show(x[j])} vs ${show(y[j])}`);
            return;
          }
        }
      }
    }
  }

  get ok(): boolean { return this.violations.length === 0; }

  private fail(property: Property, time: number, detail: string): void {
    if (this.violations.some((v) => v.property === property && v.detail === detail)) return;
    this.violations.push({ property, time, detail });
  }
}
