package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labgob"
	"cs350/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int   // leader only, index of
	lastApplied int   // leader only
	nextIndex   []int // volatile leader , index of the next log entry to send to that server
	matchIndex  []int // volatile leader, index of the highest log entry known to be replicated on server

	status      string // follower, candidate, leader
	currentTime int
	applyMsg    chan ApplyMsg
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (4A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == "leader"

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	rf.mu.Lock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		panic("Error in decoding")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
	}
	rf.mu.Unlock()
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderCommit int        // leader's commitIndex
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).

	// reply false if term < currentTerm
	// if votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote

	// fmt.Printf("RequestVote from (%d): %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.status = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	logUpToDate := false
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
		logUpToDate = args.LastLogIndex >= len(rf.log)-1
	} else {
		logUpToDate = args.LastLogTerm > rf.log[len(rf.log)-1].Term
	}

	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && logUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.currentTime = 0
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	rf.currentTime = 0

	// condition 1: reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// fmt.Printf("reject append entries from %d, term %d < current term %d\n", args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = "follower"
		rf.currentTime = 0
		rf.votedFor = -1
		return
	}

	// condition 2: reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > len(rf.log)-1 {
		// reply.mismatchIndex = len(rf.log)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// condition 3: if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	i := 0
	for i < len(args.Entries) {
		followerIndex := args.PrevLogIndex + i + 1
		if len(rf.log)-1 < followerIndex {
			break
		}
		if rf.log[followerIndex].Term != args.Entries[i].Term {
			rf.log = rf.log[:followerIndex]
			rf.persist()
			break
		}
		i++
	}

	//  4: append any new entries not already in the log
	reply.Success = true
	rf.currentTime = 0
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
	}

	// 5: if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		var newCommit int
		if args.LeaderCommit < len(rf.log)-1 {
			newCommit = args.LeaderCommit
		} else {
			newCommit = len(rf.log) - 1
		}
		for i := rf.commitIndex + 1; i <= newCommit; i++ {
			rf.commitIndex = i
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}

			// fmt.Printf("Applying to state machine (peer: %d): %d\n", rf.me, i)
			rf.applyMsg <- msg
		}
	}

}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC call
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.status == "leader"
	if isLeader {
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Command: command,
		})
		index = len(rf.log) - 1
		rf.persist()

		// fmt.Printf("New Log entry: %d\n", index)

	} else {
		return 0, 0, false
	}

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		// fmt.Printf("Start of ticker loop peer: %d, Status: %s, Term: %d, Log length: %d\n", rf.me, rf.status, rf.currentTerm, len(rf.log))
		status := rf.status

		rf.mu.Unlock()

		if status == "follower" {
			// Follower
			rf.mu.Lock()
			currentTime := rf.currentTime
			rf.mu.Unlock()
			electionTimeout := rand.Intn(200) + 300

			if currentTime > electionTimeout {
				rf.mu.Lock()
				// fmt.Printf("TIMEOUT: Follower: %d, Term: %d, CurrentTime: %d, ElectionTimeout: %d\n", rf.me, rf.currentTerm, currentTime, electionTimeout)
				rf.status = "candidate"
				rf.currentTerm++
				rf.votedFor = -1
				rf.currentTime = 0
				rf.persist()
				rf.mu.Unlock()
			}
		} else if status == "candidate" {
			// Candidate
			rf.mu.Lock()

			// fmt.Printf("Starting election Candidate: %d, Term: %d\n", rf.me, rf.currentTerm)
			peers := rf.peers
			me := rf.me
			rf.votedFor = rf.me
			term := rf.currentTerm
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term
			rf.mu.Unlock()

			votes := 0
			totalPeers := len(peers)
			finished := 0

			var wg sync.WaitGroup

			// send rpc to all peers
			for i := 0; i < totalPeers; i++ {
				if i == me {
					rf.mu.Lock()
					votes++
					finished++
					rf.mu.Unlock()
					continue
				}

				wg.Add(1)

				go func(i int) {
					args := &RequestVoteArgs{
						Term:         term,
						CandidateId:  me,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					}
					reply := &RequestVoteReply{}
					// fmt.Print("Sent request to peer: ", i, " from candidate: ", me, "\n")
					stat := rf.sendRequestVote(i, args, reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if !stat {
						finished++
						wg.Done()
						return
					}

					if reply.VoteGranted {
						votes++
						finished++
					} else {
						if reply.Term > term {
							rf.status = "follower"
							rf.currentTerm = reply.Term
							finished++
						} else {
							finished++
						}
					}
					wg.Done()
					// fmt.Print("Got reply from peer: ", i, " to candidate: ", me, "vote granted: ", reply.VoteGranted, "\n")
				}(i)
			}

			wg.Wait()
			rf.mu.Lock()

			// fmt.Printf("Gathered all votes ID: %d, Votes: %d, TotalPeers: %d, Finished %d \n", rf.me, votes, totalPeers, finished)

			if rf.status != "follower" {
				if votes > totalPeers/2 {
					// fmt.Printf("Election won Candidate: %d, Term: %d\n", rf.me, rf.currentTerm)
					rf.status = "leader"
					for peer := range peers {
						rf.nextIndex[peer] = len(rf.log)
					}
				} else {
					// fmt.Printf("Election lost Candidate: %d, Term: %d\n", rf.me, rf.currentTerm)
					rf.status = "follower"
					rf.currentTime = 0
				}
			}

			rf.persist()

			rf.mu.Unlock()

		} else if status == "leader" {
			// Leader
			rf.mu.Lock()
			// fmt.Printf("LOCK ACQUIRED: Leader: %d\n", rf.me)

			rf.currentTime = 0
			commitIndex := rf.commitIndex
			peers := rf.peers
			me := rf.me

			rf.nextIndex[me] = len(rf.log)
			rf.matchIndex[me] = len(rf.log) - 1
			log := rf.log
			term := rf.currentTerm

			rf.mu.Unlock()
			// fmt.Printf("LOCK RELEASED: Leader: %d\n", rf.me)

			// send AppendEntries RPCs to all peers
			for peer := range peers {
				if peer == me {
					continue
				}

				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}

				rf.mu.Lock()
				// fmt.Printf("LOCK ACQUIRED: Leader: %d\n", rf.me)
				args.Term = term
				args.PrevLogIndex = rf.nextIndex[peer] - 1
				args.LeaderId = me
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.LeaderCommit = rf.commitIndex
				lastLogIndex := len(log) - 1
				if rf.nextIndex[peer] <= len(log)-1 {
					args.Entries = rf.log[args.PrevLogIndex+1 : lastLogIndex+1]
				}
				rf.mu.Unlock()
				// fmt.Printf("LOCK RELEASED: Leader: %d\n", rf.me)

				go func(id int) {
					// print sending append entries to peer
					// fmt.Printf("Sending AppendEntries to peer: %d, Term: %d\n", peer, term)

					rf.mu.Lock()
					defer rf.mu.Unlock()
					// defer fmt.Printf("LOCK RELEASED 1: Leader: %d\n", rf.me)
					// fmt.Printf("LOCK ACQUIRED 1: Leader: %d\n", rf.me)
					stat := rf.sendAppendEntries(id, &args, &reply)
					if !stat {
						// fmt.Printf("LOCK RELEASED 1: Leader: %d\n", rf.me)
						return
					}

					if rf.status != "leader" || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
						// fmt.Printf("LOCK RELEASED 1: Leader: %d\n", rf.me)
						return
					}

					if reply.Success {
						// fmt.Printf("Success: AppendEntries to peer: %d, Term: %d\n", peer, term)
						// if len(log) <= rf.nextIndex[peer]+len(args.Entries) {
						// 	rf.nextIndex[peer] = len(log)
						// } else {
						// 	rf.nextIndex[peer] = rf.nextIndex[peer] + len(args.Entries)
						// }
						// rf.matchIndex[peer] = len(args.Entries) + args.PrevLogIndex
						newMatchIndex := args.PrevLogIndex + len(args.Entries)
						if newMatchIndex > rf.matchIndex[id] {
							rf.matchIndex[id] = newMatchIndex
						}
						rf.nextIndex[id] = newMatchIndex + 1

					} else {

						if reply.Term > term {
							rf.status = "follower"
							rf.currentTerm = reply.Term
							rf.votedFor = -1
						}
						rf.nextIndex[id]--
						rf.matchIndex[id] = rf.nextIndex[id] - 1

						// fmt.Printf("Rollback AppendEntries for peer: %d, Term: %d, new next index %d\n", id, term, rf.nextIndex[id])

					}

				}(peer)

			}

			rf.mu.Lock()
			// fmt.Printf("LOCK ACQUIRED 2: Leader: %d\n", rf.me)
			// check if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm
			for i := commitIndex; i <= len(log)-1; i++ {
				// fmt.Printf("Checking for commit index: %d, Term: %d\n", i, term)
				count := 0
				for j := 0; j < len(peers); j++ {
					// fmt.Printf("Checking match index: i: %d, Peer(j): %d, MatchIndex[j]: %d\n", i, j, matchIndex[j])
					if rf.matchIndex[j] >= i && log[i].Term == term {
						count++
					}
				}

				if count >= (len(peers)/2)+1 {
					// fmt.Printf("Winning majority for commit index: %d, Term: %d\n", i, term)

					// commit the log entries from commit index + 1 to i
					for currentIndex := rf.commitIndex + 1; currentIndex <= i; currentIndex++ {
						msg := ApplyMsg{
							CommandValid: true,
							Command:      log[currentIndex].Command,
							CommandIndex: currentIndex,
						}
						// print what is applied to the state machine
						// fmt.Printf("Applying to state machine (leader): %d\n", currentIndex)
						rf.applyMsg <- msg
						rf.commitIndex++
					}
				}
			}
			rf.mu.Unlock()
			// fmt.Printf("LOCK RELEASED 2: Leader: %d\n", rf.me)

		}
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		rf.currentTime += 100
		rf.mu.Unlock()

	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.applyMsg = applyCh

	// Your initialization code here (4A, 4B).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{
		{
			Term:    0,
			Command: nil,
		},
	}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = "follower"
	rf.currentTime = 0

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
