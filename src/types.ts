export type NodeId = number;
export type Role = "follower" | "candidate" | "leader";

export interface LogEntry {
  term: number;
  /** null marks the no-op a new leader appends to commit earlier entries */
  command: string | null;
}

export interface RequestVote {
  type: "RequestVote";
  term: number;
  candidateId: NodeId;
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteReply {
  type: "RequestVoteReply";
  term: number;
  voteGranted: boolean;
}

export interface AppendEntries {
  type: "AppendEntries";
  term: number;
  leaderId: NodeId;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

export interface AppendEntriesReply {
  type: "AppendEntriesReply";
  term: number;
  success: boolean;
  /** on success: the last index known to match the leader's log */
  matchIndex: number;
  /** on failure: where the leader should retry from (fast log backtracking) */
  conflictIndex: number;
}

export type Message = RequestVote | RequestVoteReply | AppendEntries | AppendEntriesReply;

export interface Envelope {
  from: NodeId;
  to: NodeId;
  msg: Message;
}

/** The state a real server would write to disk before answering any RPC. */
export interface PersistentState {
  currentTerm: number;
  votedFor: NodeId | null;
  log: LogEntry[];
}

export interface RaftConfig {
  electionTimeoutMin: number;
  electionTimeoutMax: number;
  heartbeatInterval: number;
  /** most entries sent in one AppendEntries */
  maxBatch: number;
}

export const DEFAULT_CONFIG: RaftConfig = {
  electionTimeoutMin: 150,
  electionTimeoutMax: 300,
  heartbeatInterval: 50,
  maxBatch: 16,
};

/**
 * Deliberate bugs that can be switched on to prove the chaos harness
 * catches real Raft mistakes. All are off in normal operation.
 */
export interface Bugs {
  /** Grant votes without checking the candidate's log is up to date (breaks §5.4.1). */
  voteWithoutLogCheck?: boolean;
  /** Forget votedFor on restart, as if it were never written to disk. */
  forgetVoteOnRestart?: boolean;
  /**
   * Let a leader commit entries from earlier terms by counting replicas, and
   * skip the no-op that new leaders append (it would otherwise mask this).
   * This is the scenario in Figure 8 of the Raft paper.
   */
  commitOldTerms?: boolean;
  /** Accept AppendEntries from leaders with a stale term. */
  acceptStaleLeader?: boolean;
}

export const BUG_DESCRIPTIONS: Record<keyof Bugs, string> = {
  voteWithoutLogCheck: "Vote for any candidate, even one whose log is missing committed entries",
  forgetVoteOnRestart: "Forget who you voted for when you restart",
  commitOldTerms: "Commit entries from earlier terms by counting replicas, without a new-term no-op (the Figure 8 bug)",
  acceptStaleLeader: "Accept log entries from a leader with an out-of-date term",
};
