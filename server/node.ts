/**
 * A real Raft server: the same RaftNode the simulator runs, wired to a real
 * clock, real HTTP, and a real disk.
 *
 *   NODE_ID=1 PORT=7001 PEERS="2=http://localhost:7002,3=..." DATA_DIR=./data node build/server/node.js
 *
 * Endpoints:
 *   POST /raft     a Raft message from a peer: { from, msg }
 *   POST /submit   a client command: { command } (answers once committed, or redirects to the leader)
 *   GET  /status   this server's state as JSON
 */
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import http from "node:http";
import { join } from "node:path";
import { DEFAULT_CONFIG, type Message, type PersistentState, RaftNode, Rng } from "../src/index.js";

const id = Number(process.env.NODE_ID);
const port = Number(process.env.PORT ?? 7000);
const peers = new Map<number, string>(
  (process.env.PEERS ?? "").split(",").filter(Boolean).map((p) => {
    const [pid, url] = p.split("=");
    return [Number(pid), url] as [number, string];
  }),
);
const dataDir = process.env.DATA_DIR ?? "./data";
const stateFile = join(dataDir, `node-${id}.json`);
if (!id || peers.size === 0) {
  console.error("set NODE_ID, PORT and PEERS (for example PEERS=\"2=http://localhost:7002,3=http://localhost:7003\")");
  process.exit(2);
}

const started = performance.now();
const now = () => Math.floor(performance.now() - started);

mkdirSync(dataDir, { recursive: true });
const saved: PersistentState | undefined = existsSync(stateFile) ? JSON.parse(readFileSync(stateFile, "utf8")) : undefined;
const node = new RaftNode(id, [...peers.keys()], DEFAULT_CONFIG, new Rng((id * 2654435761) ^ Date.now()), {}, now(), saved);
log(saved ? `restarted from disk: term ${saved.currentTerm}, ${saved.log.length - 1} entries` : "started fresh");

let lastSaved = JSON.stringify(node.persistentState());

/** After every input: save state to disk *before* sending anything (Raft requires it), then send. */
function pump(): void {
  const state = JSON.stringify(node.persistentState());
  if (state !== lastSaved) {
    // write-then-rename, so a crash mid-write can't leave a torn file
    writeFileSync(stateFile + ".tmp", state);
    renameSync(stateFile + ".tmp", stateFile);
    lastSaved = state;
  }
  for (const ev of node.takeEvents()) {
    if (ev.kind === "becameLeader") log(`became leader for term ${ev.term} with ${ev.votes} votes`);
    if (ev.kind === "electionStarted") log(`starting an election for term ${ev.term}`);
    if (ev.kind === "steppedDown") log(`stepped down: ${ev.reason}`);
    if (ev.kind === "applied" && ev.entry.command !== null) log(`applied #${ev.index}: ${ev.entry.command}`);
  }
  for (const env of node.takeMessages()) {
    const url = peers.get(env.to);
    if (!url) continue;
    fetch(`${url}/raft`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ from: id, msg: env.msg }),
      signal: AbortSignal.timeout(300),
    }).catch(() => { /* a lost message: Raft retries on its own */ });
  }
}

setInterval(() => { node.tick(now()); pump(); }, 10);

function body(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
  });
}

function json(res: http.ServerResponse, status: number, value: unknown): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(value));
}

http.createServer(async (req, res) => {
  try {
    if (req.method === "POST" && req.url === "/raft") {
      const { from, msg } = (await body(req)) as { from: number; msg: Message };
      node.receive(from, msg, now());
      pump();
      res.writeHead(204).end();
    } else if (req.method === "POST" && req.url === "/submit") {
      const { command } = await body(req);
      const index = node.submit(String(command), now());
      pump();
      if (index === null) return json(res, 421, { error: "not the leader", leader: node.leaderId, leaderUrl: node.leaderId ? peers.get(node.leaderId) : null });
      const term = node.currentTerm;
      const deadline = Date.now() + 3000;
      const wait = () => {
        if (node.commitIndex >= index && node.log[index]?.term === term) return json(res, 200, { index, term });
        if (node.currentTerm !== term || Date.now() > deadline) return json(res, 503, { error: "lost leadership before committing; retry" });
        setTimeout(wait, 5);
      };
      wait();
    } else if (req.method === "GET" && req.url === "/status") {
      json(res, 200, {
        id, role: node.role, term: node.currentTerm, leader: node.leaderId,
        commitIndex: node.commitIndex,
        log: node.log.slice(1).map((e, i) => ({ index: i + 1, term: e.term, command: e.command })),
      });
    } else {
      json(res, 404, { error: "not found" });
    }
  } catch (e) {
    json(res, 400, { error: String(e) });
  }
}).listen(port, () => log(`listening on :${port}`));

function log(text: string): void {
  console.log(`[S${id} ${now().toString().padStart(6)}ms] ${text}`);
}
