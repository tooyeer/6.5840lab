package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
func Max(x, y int) int {
	if x < y {
		return y
	} else {
		return x
	}
}

const (

	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 200 //500
	MinVoteTime  = 150

	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 80 //200
	AppliedSleep   = 50 //300
)

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(MoreVoteTime+rand.Intn(MinVoteTime)) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatSleep) * time.Millisecond
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) getlastLogIndex() int {
	return len(rf.logs) + rf.SnapShopIndex
}

func (rf *Raft) getlastLogTerm() int {
	if len(rf.logs) == 0 {
		return rf.SnapShopTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) getLogTerm(index int) int {
	if index <= rf.SnapShopIndex || index > len(rf.logs)+rf.SnapShopIndex {
		return rf.SnapShopTerm
	} else {
		return rf.logs[index-1-rf.SnapShopIndex].Term
	}
}

func (rf *Raft) getLogCommand(index int) interface{} {
	if index < rf.SnapShopIndex || index > len(rf.logs)+rf.SnapShopIndex {
		return nil
	} else {
		return rf.logs[index-1-rf.SnapShopIndex].Command
	}
}
