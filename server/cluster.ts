/**
 * Launch a real 5-server cluster on this machine and put it through its paces:
 * write data, kill the leader, keep writing, restart it, and check every server
 * ends up with the identical log.
 *
 *   npm run cluster
 */
import { type ChildProcess, fork } from "node:child_process";
import { rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const ids = [1, 2, 3, 4, 5];
const url = (id: number) => `http://127.0.0.1:${7100 + id}`;
const dataDir = join(here, "..", "..", ".cluster-data");
rmSync(dataDir, { recursive: true, force: true });
const procs = new Map<number, ChildProcess>();
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function start(id: number): void {
  const p = fork(join(here, "node.js"), [], {
    env: {
      ...process.env, NODE_ID: String(id), PORT: String(7100 + id), DATA_DIR: dataDir,
      PEERS: ids.filter((p) => p !== id).map((p) => `${p}=${url(p)}`).join(","),
    },
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  procs.set(id, p);
}

async function status(id: number): Promise<any | null> {
  try { return await (await fetch(`${url(id)}/status`, { signal: AbortSignal.timeout(500) })).json(); } catch { return null; }
}

async function waitForLeader(exclude?: number): Promise<number> {
  for (let i = 0; i < 100; i++) {
    for (const id of ids) {
      if (id === exclude) continue;
      const s = await status(id);
      if (s?.role === "leader") return id;
    }
    await sleep(100);
  }
  throw new Error("no leader elected within 10 seconds");
}

/** Submit through any server, following redirects to the leader like a real client. */
async function submit(command: string): Promise<void> {
  let target = ids[0];
  for (let attempt = 0; attempt < 40; attempt++) {
    try {
      const res = await fetch(`${url(target)}/submit`, {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ command }), signal: AbortSignal.timeout(4000),
      });
      const reply = await res.json();
      if (res.status === 200) { console.log(`  ✓ "${command}" committed at index ${reply.index}`); return; }
      if (res.status === 421 && reply.leader) target = reply.leader;
      else target = ids[(ids.indexOf(target) + 1) % ids.length];
    } catch {
      target = ids[(ids.indexOf(target) + 1) % ids.length];
    }
    await sleep(100);
  }
  throw new Error(`couldn't commit "${command}"`);
}

async function main(): Promise<void> {
  console.log("Starting 5 Raft servers...");
  ids.forEach(start);
  const first = await waitForLeader();
  console.log(`\nS${first} is the leader. Writing 5 commands:`);
  for (let i = 1; i <= 5; i++) await submit(`set a=${i}`);

  console.log(`\nKilling the leader, S${first}, with SIGKILL...`);
  procs.get(first)!.kill("SIGKILL");
  const second = await waitForLeader(first);
  console.log(`\nS${second} took over. Writing 5 more commands with S${first} down:`);
  for (let i = 1; i <= 5; i++) await submit(`set b=${i}`);

  console.log(`\nRestarting S${first}; it should reload its log from disk and catch up...`);
  start(first);
  await sleep(1500);

  const states = await Promise.all(ids.map(status));
  const committed = states.map((s) => s.log.slice(0, s.commitIndex).map((e: any) => `${e.term}:${e.command}`).join(","));
  const commands = states[0].log.filter((e: any) => e.command).map((e: any) => e.command);
  const agree = committed.every((c) => c === committed[0]);
  const complete = commands.length === 10;
  console.log(`\nCommitted logs identical on all 5 servers: ${agree ? "yes" : "NO"}`);
  console.log(`All 10 commands present, in order: ${complete ? "yes" : "NO"} (${commands.join(", ")})`);
  for (const p of procs.values()) p.kill("SIGKILL");
  rmSync(dataDir, { recursive: true, force: true });
  process.exit(agree && complete ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  for (const p of procs.values()) p.kill("SIGKILL");
  process.exit(1);
});
