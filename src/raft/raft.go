package raft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type Status int
type VoteState int
type AppendEntriesState int

// 枚举节点的类型：跟随者、竞选者、领导者
const (
	Follower Status = iota
	Candidate
	Leader
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	UpIndex int
	UpTerm  int
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludeTerm   int
	Offset            int
	Data              []byte
	Done              bool
}
type InstallSnapshotReply struct {
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	applyChan chan ApplyMsg // 用来写入通道

	nextIndex  []int
	matchIndex []int

	status         Status
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	SnapShopIndex int
	SnapShopTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.persistData()
	rf.persister.Save(data, rf.persister.ReadSnapshot())
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.SnapShopIndex)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.logs) != nil || d.Decode(&rf.SnapShopIndex) != nil {
		fmt.Printf("[  %v  ] Decode error.........\n", rf.me)
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	if lastIncludedIndex > rf.SnapShopIndex {
		remove := lastIncludedIndex - rf.SnapShopIndex
		rf.SnapShopIndex = lastIncludedIndex
		rf.SnapShopTerm = lastIncludedTerm
		if len(rf.logs) > remove {
			rf.logs = rf.logs[remove:]
		} else {
			rf.logs = rf.logs[0:0]
		}
		rf.commitIndex = lastIncludedIndex
		rf.persister.Save(rf.persistData(), snapshot)
		DPrintf("{Node %v} CondInstallSnapshot logs:%v , rf.SnapShopIndex %v , lastIncludedIndex %v", rf.me, rf.logs, rf.SnapShopIndex, lastIncludedIndex)
	}
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	remove := index - rf.SnapShopIndex
	rf.SnapShopTerm = rf.getLogTerm(index)
	rf.SnapShopIndex = index
	rf.logs = rf.logs[remove:]
	rf.persister.Save(rf.persistData(), snapshot)
	DPrintf("{Node %v} do a Snapshop[SnapshopIndex:%v , rf.logs:%v , SnapShotTerm:%v ]  in term %v", rf.me, rf.SnapShopIndex, rf.logs, rf.SnapShopTerm, rf.currentTerm)
	return

}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()

	defer rf.mu.Unlock()

	if rf.killed() {
		return index, term, false
	}

	// 如果不是leader，直接返回
	if rf.status != Leader {
		return index, term, false
	}

	// 初始化日志条目。并进行追加
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, appendLog)
	rf.persist()
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, appendLog, rf.currentTerm)

	return rf.getlastLogIndex(), rf.currentTerm, isLeader
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyChan = applyCh

	rf.SnapShopTerm = 0
	rf.SnapShopIndex = 0

	rf.status = Follower
	rf.heartbeatTimer = time.NewTimer(RandomizedElectionTimeout())
	rf.electionTimer = time.NewTimer(StableHeartbeatTimeout())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, rf.getlastLogIndex()+1
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyTicker()

	return rf
}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			{
				if rf.killed() {
					break
				}
				rf.mu.Lock()
				rf.status = Candidate
				rf.sendElection()
				rf.electionTimer.Reset(RandomizedElectionTimeout())
				rf.mu.Unlock()
			}
		case <-rf.heartbeatTimer.C:
			{
				if rf.killed() {
					break
				}
				rf.mu.Lock()
				if rf.status == Leader {
					rf.BroadcastHeartbeat()
					rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
					rf.electionTimer.Reset(RandomizedElectionTimeout())
				}
				rf.mu.Unlock()
			}
		}
	}
	return
}

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		time.Sleep(time.Duration(AppliedSleep) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid:  false,
					Command:       nil,
					CommandIndex:  0,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				if rf.lastApplied <= rf.SnapShopIndex {
					rf.lastApplied = rf.SnapShopIndex
					applyMsg = ApplyMsg{
						SnapshotValid: true,
						Snapshot:      rf.persister.ReadSnapshot(),
						SnapshotTerm:  rf.SnapShopTerm,
						SnapshotIndex: rf.SnapShopIndex,
					}
				} else {
					applyMsg = ApplyMsg{
						CommandValid: true,
						CommandIndex: rf.lastApplied,
						Command:      rf.getLogCommand(rf.lastApplied),
					}
				}

				DPrintf("{Node %v} commit a command %v", rf.me, applyMsg)
				rf.mu.Unlock()
				rf.applyChan <- applyMsg
				rf.mu.Lock()
			}
		}
		rf.mu.Unlock()

	}
	return
}

func (rf *Raft) sendElection() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	voteNums := 1
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getlastLogIndex(),
		rf.getlastLogTerm(),
	}
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, args)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{
				VoteGranted: false,
				Term:        -1,
			}
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				if args.Term == rf.currentTerm && rf.status == Candidate {
					if reply.VoteGranted == true {
						voteNums += 1
						if voteNums >= len(rf.peers)/2+1 {
							voteNums = 0
							rf.status = Leader
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = rf.getlastLogIndex() + 1
								rf.matchIndex[i] = 0
							}
							rf.BroadcastHeartbeat()
							rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
							rf.mu.Unlock()
							return
						}
					} else {
						if reply.Term > rf.currentTerm || reply.Term == -1 {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
							}
							rf.status = Follower
							rf.votedFor = -1
							rf.persist()
							DPrintf("{Node %v} cannot be Leader\n", rf.me)
							rf.mu.Unlock()
							return
						}
					}
				}
				rf.mu.Unlock()
				return
			}
			return
		}(peer)
	}
	return
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,lastLogIndex %v,lastLogTerm %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.status, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getlastLogIndex(), rf.getlastLogTerm(), args, reply)

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && (rf.votedFor != -1 && rf.votedFor != args.CandidateId)) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.persist()
	}
	lastLogTerm := rf.getlastLogTerm()
	if (args.LastLogTerm < lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < rf.getlastLogIndex()) {
		reply.Term, reply.VoteGranted = -1, false
		return
	} else {
		rf.votedFor = args.CandidateId
		rf.status = Follower
		rf.persist()
		reply.Term, reply.VoteGranted = rf.currentTerm, true

	}
}

func (rf *Raft) BroadcastHeartbeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			nextIndex := rf.nextIndex[peer]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if rf.getlastLogIndex()+1 < nextIndex {
				nextIndex = rf.getlastLogIndex()
			}
			if nextIndex <= rf.SnapShopIndex {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.SnapShopIndex,
					LastIncludeTerm:   rf.SnapShopTerm,
					Offset:            0,
					Data:              rf.persister.ReadSnapshot(),
					Done:              false,
				}
				reply := InstallSnapshotReply{Term: 0}
				if rf.sendInstallSnapshot(peer, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					defer DPrintf("{Node %v}  send snapshot to %v  args:%v reply:%v", rf.me, peer, args, reply)
					if args.Term == rf.currentTerm && rf.status == Leader {
						if reply.Term > rf.currentTerm {
							rf.status = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
						} else {
							match := args.LastIncludedIndex
							rf.matchIndex[peer] = Max(match, rf.matchIndex[peer])
							rf.nextIndex[peer] = Max(match+1, rf.nextIndex[peer])
							rf.updateLeaderCommit()
							DPrintf("{Node %v} .nextIndex[%v] = %v after snapshot", rf.me, peer, rf.nextIndex[peer])
						}
					}
					return
				}
				return
			} else {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      make([]LogEntry, rf.getlastLogIndex()-nextIndex+1),
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{
					Term:    -1,
					Success: false,
					UpIndex: 0,
					UpTerm:  0,
				}
				copy(args.Entries, rf.logs[nextIndex-1-rf.SnapShopIndex:])
				if rf.nextIndex[peer] > 1 {
					args.PrevLogIndex = rf.nextIndex[peer] - 1
				}
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
				}
				args.Entries = rf.logs[rf.nextIndex[peer]-1-rf.SnapShopIndex:]
				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if args.Term == rf.currentTerm && rf.status == Leader {
						if reply.Success == false {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.status = Follower
								rf.persist()
								return
							} else {
								if rf.getLogTerm(reply.UpIndex) == reply.UpTerm {
									rf.matchIndex[peer] = reply.UpIndex
									rf.nextIndex[peer] = reply.UpIndex + 1
								} else {
									rf.nextIndex[peer] = reply.UpIndex
								}
							}
							return
						} else {
							match := args.PrevLogIndex + len(args.Entries)
							next := match + 1
							rf.matchIndex[peer] = Max(rf.matchIndex[peer], match)
							rf.nextIndex[peer] = Max(rf.nextIndex[peer], next)
							rf.updateLeaderCommit()
						}
						DPrintf("{Node %v} .nextIndex[%v] = %v  here", rf.me, peer, rf.nextIndex[peer])
						return
					}
				}
			}
		}(peer)
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v , SnapShopIndex %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.status, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.SnapShopIndex, rf.getlastLogIndex(), args, reply)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.status = Follower
	rf.votedFor = args.LeaderId
	rf.currentTerm = args.Term
	rf.persist()
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	if rf.getlastLogIndex() < args.PrevLogIndex {
		reply.UpIndex = rf.getlastLogIndex()
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpTerm = rf.getlastLogTerm()
		return
	} else if len(rf.logs) > 0 && rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		index := args.PrevLogIndex
		for rf.getLogTerm(index) == rf.getLogTerm(args.PrevLogIndex) {
			index--
		}
		reply.UpIndex = index
		reply.UpTerm = rf.getLogTerm(reply.UpIndex)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.logs = rf.logs[:args.PrevLogIndex-rf.SnapShopIndex]
	rf.persist()
	if args.Entries != nil {
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.commitIndex = Min(rf.getlastLogIndex(), args.LeaderCommit)
	return
}

func (rf *Raft) updateLeaderCommit() {
	x := rf.commitIndex + 1
	for {
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}
			if rf.matchIndex[i] >= x {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			x++
		} else {
			break
		}
	}
	x--
	if x > rf.commitIndex && rf.getLogTerm(x) == rf.currentTerm {
		rf.commitIndex = x
	}
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.SnapShopIndex {
		return
	}
	rf.status = Follower
	rf.votedFor = args.LeaderId
	rf.currentTerm = args.Term
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	rf.CondInstallSnapshot(args.LastIncludeTerm, args.LastIncludedIndex, args.Data)
	defer DPrintf("{Node %v}  get InstallSnapshot from %v  Term:%v rf.SnapShopIndex:%v", rf.me, args.LeaderId, rf.currentTerm, rf.SnapShopIndex)
	return
}
