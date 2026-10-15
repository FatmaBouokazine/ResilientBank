--------------------------------- MODULE Raft ---------------------------------
(***************************************************************************)
(* Formal specification of the Raft Consensus Protocol for ResilientBank.  *)
(*                                                                         *)
(* ResilientBank relies on Raft consensus in Phase 3 for active-active     *)
(* dual-region replication, leader election, and resilient log durability. *)
(* This TLA+ specification models:                                         *)
(*   - Leader Election & Term Monotonicity                                 *)
(*   - Quorum-based voting and Split-Vote Handling                         *)
(*   - Log Replication via nextIndex/matchIndex tracking                  *)
(*   - Log Matching and Conflict Resolution                                *)
(*   - Commit Index Advancement via Quorum Acknowledgement                 *)
(*                                                                         *)
(* Verified Core Safety Invariants:                                        *)
(*   1. ElectionSafety: At most one leader can be elected per term.        *)
(*   2. LogMatching: If two logs contain an entry with the same index and  *)
(*      term, then the logs are identical in all entries up to that index. *)
(*   3. LeaderCompleteness: If a log entry is committed in a given term,   *)
(*      that entry is present in the logs of the leaders for all higher    *)
(*      terms.                                                             *)
(*   4. CommitIndexBounded: commitIndex never exceeds log length.          *)
(*   5. TypeOK: All state variables remain within their valid domains.     *)
(***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS 
    Server,         \* Set of participating replica nodes (e.g. {s1, s2, s3})
    Value,          \* Set of commands/transaction payloads (e.g. {v1, v2})
    MaxTerm,        \* Bound on term numbers for finite model checking
    MaxLogLen       \* Bound on log length for finite model checking

CONSTANTS Nil

VARIABLES
    currentTerm,    \* [Server -> 1..MaxTerm]
    state,          \* [Server -> {"Follower", "Candidate", "Leader"}]
    votedFor,       \* [Server -> Server \cup {Nil}]
    log,            \* [Server -> Seq([term: 1..MaxTerm, value: Value])]
    commitIndex,    \* [Server -> 0..MaxLogLen]
    votesGranted,   \* [Server -> SUBSET Server]
    nextIndex,      \* [Server -> [Server -> 1..(MaxLogLen+1)]]
    matchIndex,     \* [Server -> [Server -> 0..MaxLogLen]]
    messages        \* Set of in-flight network messages

vars == <<currentTerm, state, votedFor, log, commitIndex, votesGranted, nextIndex, matchIndex, messages>>

-----------------------------------------------------------------------------
(* Helper Definitions *)

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

\* Majority quorum predicate
IsQuorum(S) == Cardinality(S) * 2 > Cardinality(Server)

\* Log query helpers
LastTerm(s) == 
    IF Len(log[s]) = 0 THEN 0 ELSE log[s][Len(log[s])].term

LastIndex(s) == Len(log[s])

\* True if candidate log is at least as up-to-date as receiver log (Section 5.4.1)
IsLogUpToDate(cLastTerm, cLastIndex, s) ==
    \/ cLastTerm > LastTerm(s)
    \/ (cLastTerm = LastTerm(s) /\ cLastIndex >= LastIndex(s))

-----------------------------------------------------------------------------
(* Type Invariant *)

TypeOK ==
    /\ currentTerm \in [Server -> 1..MaxTerm]
    /\ state \in [Server -> {"Follower", "Candidate", "Leader"}]
    /\ votedFor \in [Server -> Server \cup {Nil}]
    /\ \A s \in Server : 
        /\ Len(log[s]) <= MaxLogLen
        /\ \A i \in 1..Len(log[s]) :
            log[s][i].term \in 1..MaxTerm /\ log[s][i].value \in Value
    /\ commitIndex \in [Server -> 0..MaxLogLen]
    /\ votesGranted \in [Server -> SUBSET Server]
    /\ nextIndex \in [Server -> [Server -> 1..(MaxLogLen+1)]]
    /\ matchIndex \in [Server -> [Server -> 0..MaxLogLen]]

-----------------------------------------------------------------------------
(* Initial State *)

Init ==
    /\ currentTerm = [s \in Server |-> 1]
    /\ state = [s \in Server |-> "Follower"]
    /\ votedFor = [s \in Server |-> Nil]
    /\ log = [s \in Server |-> <<>>]
    /\ commitIndex = [s \in Server |-> 0]
    /\ votesGranted = [s \in Server |-> {}]
    /\ nextIndex = [s \in Server |-> [d \in Server |-> 1]]
    /\ matchIndex = [s \in Server |-> [d \in Server |-> 0]]
    /\ messages = {}

-----------------------------------------------------------------------------
(* State Transition Actions *)

\* 1. Election Timeout: Follower or Candidate transitions to Candidate
Timeout(s) ==
    /\ state[s] \in {"Follower", "Candidate"}
    /\ currentTerm[s] < MaxTerm
    /\ ~ \E m \in messages : m.type = "RequestVoteReq" /\ m.candidate = s
    /\ currentTerm' = [currentTerm EXCEPT ![s] = @ + 1]
    /\ state' = [state EXCEPT ![s] = "Candidate"]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {s}]
    /\ messages' = messages \cup {
        [type           |-> "RequestVoteReq",
         term           |-> currentTerm[s] + 1,
         candidate      |-> s,
         lastLogIndex   |-> LastIndex(s),
         lastLogTerm    |-> LastTerm(s),
         dest           |-> d] : d \in Server \ {s}
       }
    /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex>>

\* 2. RequestVote Request Handler
HandleRequestVoteReq(s, m) ==
    /\ m.type = "RequestVoteReq"
    /\ m.dest = s
    /\ LET newTerm == Max(currentTerm[s], m.term)
           canVote == 
                /\ m.term >= currentTerm[s]
                /\ (votedFor[s] = Nil \/ votedFor[s] = m.candidate \/ m.term > currentTerm[s])
                /\ IsLogUpToDate(m.lastLogTerm, m.lastLogIndex, s)
       IN
       /\ currentTerm' = [currentTerm EXCEPT ![s] = newTerm]
       /\ state' = [state EXCEPT ![s] = IF m.term > currentTerm[s] THEN "Follower" ELSE @]
       /\ votedFor' = [votedFor EXCEPT ![s] = IF canVote THEN m.candidate 
                                              ELSE IF m.term > currentTerm[s] THEN Nil ELSE @]
       /\ messages' = (messages \ {m}) \cup {
            [type        |-> "RequestVoteResp",
             term        |-> newTerm,
             from        |-> s,
             voteGranted |-> canVote,
             dest        |-> m.candidate]
          }
       /\ UNCHANGED <<log, commitIndex, votesGranted, nextIndex, matchIndex>>

\* 3. RequestVote Response Handler: Become Leader on Quorum
HandleRequestVoteResp(s, m) ==
    /\ m.type = "RequestVoteResp"
    /\ m.dest = s
    /\ IF m.term > currentTerm[s] THEN
          \* Discover higher term; revert to Follower
          /\ currentTerm' = [currentTerm EXCEPT ![s] = m.term]
          /\ state' = [state EXCEPT ![s] = "Follower"]
          /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
          /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
          /\ messages' = messages \ {m}
          /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex>>
       ELSE IF state[s] = "Candidate" /\ m.term = currentTerm[s] /\ m.voteGranted THEN
          LET newVotes == votesGranted[s] \cup {m.from}
              wonElection == IsQuorum(newVotes)
          IN
          /\ votesGranted' = [votesGranted EXCEPT ![s] = newVotes]
          /\ state' = [state EXCEPT ![s] = IF wonElection THEN "Leader" ELSE @]
          /\ nextIndex' = [nextIndex EXCEPT ![s] = IF wonElection 
                                                   THEN [d \in Server |-> Len(log[s]) + 1] 
                                                   ELSE @]
          /\ matchIndex' = [matchIndex EXCEPT ![s] = IF wonElection 
                                                     THEN [d \in Server |-> 0] 
                                                     ELSE @]
          /\ messages' = messages \ {m}
          /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex>>
       ELSE
          /\ messages' = messages \ {m}
          /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, votesGranted, nextIndex, matchIndex>>

\* 4. Client Request: Leader accepts transaction command into log
ClientRequest(s, v) ==
    /\ state[s] = "Leader"
    /\ Len(log[s]) < MaxLogLen
    /\ LET entry == [term |-> currentTerm[s], value |-> v] IN
       log' = [log EXCEPT ![s] = Append(@, entry)]
    /\ UNCHANGED <<currentTerm, state, votedFor, commitIndex, votesGranted, nextIndex, matchIndex, messages>>

\* 5. Leader broadcasts AppendEntries to replicate log
SendAppendEntries(s, dest) ==
    /\ state[s] = "Leader"
    /\ dest # s
    /\ ~ \E m \in messages : m.type = "AppendEntriesReq" /\ m.leader = s /\ m.dest = dest
    /\ LET prevIndex == nextIndex[s][dest] - 1
           prevTerm  == IF prevIndex = 0 THEN 0 ELSE log[s][prevIndex].term
           entries   == IF nextIndex[s][dest] <= Len(log[s])
                        THEN SubSeq(log[s], nextIndex[s][dest], Len(log[s]))
                        ELSE <<>>
       IN
       /\ messages' = messages \cup {
            [type         |-> "AppendEntriesReq",
             term         |-> currentTerm[s],
             leader       |-> s,
             prevLogIndex |-> prevIndex,
             prevLogTerm  |-> prevTerm,
             entries      |-> entries,
             leaderCommit |-> commitIndex[s],
             dest         |-> dest]
          }
       /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, votesGranted, nextIndex, matchIndex>>

\* 6. AppendEntries Request Handler
HandleAppendEntriesReq(s, m) ==
    /\ m.type = "AppendEntriesReq"
    /\ m.dest = s
    /\ IF m.term < currentTerm[s] THEN
          \* Reject stale leader
          /\ messages' = (messages \ {m}) \cup {
                [type       |-> "AppendEntriesResp",
                 term       |-> currentTerm[s],
                 from       |-> s,
                 success    |-> FALSE,
                 matchIndex |-> 0,
                 dest       |-> m.leader]
             }
          /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, votesGranted, nextIndex, matchIndex>>
       ELSE
          LET newTerm == m.term
              logConsistent == 
                \/ m.prevLogIndex = 0
                \/ (m.prevLogIndex <= Len(log[s]) /\ log[s][m.prevLogIndex].term = m.prevLogTerm)
          IN
          /\ currentTerm' = [currentTerm EXCEPT ![s] = newTerm]
          /\ state' = [state EXCEPT ![s] = "Follower"]
          /\ votedFor' = [votedFor EXCEPT ![s] = IF m.term > currentTerm[s] THEN Nil ELSE @]
          /\ IF logConsistent THEN
                LET updatedLog == 
                    IF Len(m.entries) = 0 THEN
                        log[s]
                    ELSE
                        LET combined == SubSeq(log[s], 1, m.prevLogIndex) \o m.entries
                        IN
                        \* Do not truncate if follower already has identical matching longer log
                        IF Len(log[s]) >= Len(combined) /\ SubSeq(log[s], 1, Len(combined)) = combined
                        THEN log[s]
                        ELSE IF Len(combined) > MaxLogLen
                        THEN SubSeq(combined, 1, MaxLogLen)
                        ELSE combined
                    newCommit == 
                        IF m.leaderCommit > commitIndex[s]
                        THEN Min(m.leaderCommit, Len(updatedLog))
                        ELSE commitIndex[s]
                IN
                /\ log' = [log EXCEPT ![s] = updatedLog]
                /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommit]
                /\ messages' = (messages \ {m}) \cup {
                    [type       |-> "AppendEntriesResp",
                     term       |-> newTerm,
                     from       |-> s,
                     success    |-> TRUE,
                     matchIndex |-> Len(updatedLog),
                     dest       |-> m.leader]
                   }
             ELSE
                \* Inconsistent log; reject so leader decrements nextIndex
                /\ messages' = (messages \ {m}) \cup {
                    [type       |-> "AppendEntriesResp",
                     term       |-> newTerm,
                     from       |-> s,
                     success    |-> FALSE,
                     matchIndex |-> 0,
                     dest       |-> m.leader]
                   }
                /\ UNCHANGED <<log, commitIndex>>
          /\ UNCHANGED <<votesGranted, nextIndex, matchIndex>>

\* 7. AppendEntries Response Handler: Leader advances commitIndex
HandleAppendEntriesResp(s, m) ==
    /\ m.type = "AppendEntriesResp"
    /\ m.dest = s
    /\ IF m.term > currentTerm[s] THEN
          \* Discover higher term; revert to Follower
          /\ currentTerm' = [currentTerm EXCEPT ![s] = m.term]
          /\ state' = [state EXCEPT ![s] = "Follower"]
          /\ votedFor' = [votedFor EXCEPT ![s] = Nil]
          /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
          /\ messages' = messages \ {m}
          /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex>>
       ELSE IF state[s] = "Leader" /\ m.term = currentTerm[s] THEN
          /\ messages' = messages \ {m}
          /\ IF m.success THEN
                LET newNext  == [nextIndex[s] EXCEPT ![m.from] = m.matchIndex + 1]
                    newMatch == [matchIndex[s] EXCEPT ![m.from] = Max(@, m.matchIndex)]
                    \* Count quorum for log entries in leader's term
                    canCommit(idx) ==
                        /\ idx > commitIndex[s]
                        /\ idx <= Len(log[s])
                        /\ log[s][idx].term = currentTerm[s]
                        /\ IsQuorum({n \in Server : n = s \/ newMatch[n] >= idx})
                    newCommit == 
                        IF \E idx \in (commitIndex[s]+1)..Len(log[s]) : canCommit(idx)
                        THEN CHOOSE idx \in (commitIndex[s]+1)..Len(log[s]) : 
                                canCommit(idx) /\ \A other \in (commitIndex[s]+1)..Len(log[s]) : canCommit(other) => idx >= other
                        ELSE commitIndex[s]
                IN
                /\ nextIndex' = [nextIndex EXCEPT ![s] = newNext]
                /\ matchIndex' = [matchIndex EXCEPT ![s] = newMatch]
                /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommit]
             ELSE
                \* Rejected; decrement nextIndex
                /\ nextIndex' = [nextIndex EXCEPT ![s][m.from] = Max(1, @ - 1)]
                /\ UNCHANGED <<matchIndex, commitIndex>>
          /\ UNCHANGED <<currentTerm, state, votedFor, log, votesGranted>>
       ELSE
          /\ messages' = messages \ {m}
          /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, votesGranted, nextIndex, matchIndex>>

-----------------------------------------------------------------------------
(* Specification *)

Next ==
    \/ \E s \in Server : Timeout(s)
    \/ \E s \in Server, v \in Value : ClientRequest(s, v)
    \/ \E s, dest \in Server : SendAppendEntries(s, dest)
    \/ \E m \in messages : 
        \/ HandleRequestVoteReq(m.dest, m)
        \/ HandleRequestVoteResp(m.dest, m)
        \/ HandleAppendEntriesReq(m.dest, m)
        \/ HandleAppendEntriesResp(m.dest, m)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Core Safety Invariants *)

\* Invariant 1: Election Safety
\* At most one leader can be elected per term.
ElectionSafety ==
    \A s1, s2 \in Server :
        (state[s1] = "Leader" /\ state[s2] = "Leader" /\ currentTerm[s1] = currentTerm[s2])
        => s1 = s2

\* Invariant 2: Log Matching
\* If two logs contain an entry with identical index and term, the logs are identical
\* in all entries up to that index.
LogMatching ==
    \A s1, s2 \in Server :
        \A idx \in 1..Min(Len(log[s1]), Len(log[s2])) :
            (log[s1][idx].term = log[s2][idx].term) =>
                (\A i \in 1..idx : log[s1][i] = log[s2][i])

\* Invariant 3: Leader Completeness
\* If a log entry is committed in a given term, that entry is present in the logs
\* of leaders for all higher terms.
LeaderCompleteness ==
    \A s1, s2 \in Server :
        \A idx \in 1..commitIndex[s1] :
            (state[s2] = "Leader" /\ currentTerm[s2] > log[s1][idx].term) =>
                (Len(log[s2]) >= idx /\ log[s2][idx] = log[s1][idx])

\* Invariant 4: Commit Index Bounded
\* commitIndex never exceeds the length of the local log.
CommitIndexBounded ==
    \A s \in Server : commitIndex[s] <= Len(log[s])

=============================================================================
