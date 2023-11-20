# 6.5840lab

## 时间线
- lab1： 10/24
- lab2A: 11/03
- lab2B: 11/06
- lab2C: 11/12
- lab2D: 11/15
- ......

## lab2思路
[Raft](http://nil.csail.mit.edu/6.5840/2023/papers/raft-extended.pdf)是一种用于管理复制日志的算法。它作用在状态机的更下层一些，用于实现多个副本对状态机日志操作达成分布式共识，并且实现了线性一致性写。有三个关键。
1. “ data flows in a simple fashion from the leader to other servers” ，数据以简单的方式从领导者服务器流向其他服务器。 
2. leader的Term和每个服务器日志的index。包括实现日志匹配（Log Matching，5.3）：
- If two entries in different logs have the same index and term, then they store the same command.
（如果不同日志中的两个条目具有相同的索引和术语，则它们存储相同的命令。）
- If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.
（如果不同日志中的两个条目具有相同的索引和术语，则这些日志在所有前面的条目中都是相同的。）
3. “Volatile state on leaders”中用于实现控制副本日志向领导者同步的乐观的nextIndex[]和用于统计副本状态机达到状态的悲观的mathIndex[] (fig8的问题需要依赖它)。

Raft中存在三种RPC，用于实现选举通讯的RequestVote RPC、用于实现心跳保活和向副本发送日志的AppendEntries RPC、用于实现leader向follower发送日志快照的InstallSnapshot RPC。Figure 2和Figure 13给出了三个RPC的结构和处理逻辑，遵守就好。
### 实现思路
节点有Follower、Leader、Candidater三种角色。RequestVote由Candidater发出，各节点接收，获得一半以上选票，可以升级成为Leader。AppendEntries由Leader后发出，各节点接收。用Time.Timer实现定时器。将选举定时任务和心跳定时任务放在一个ticker中。

```
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
```
选举定时器的重置发生在三种情况下：
1. Candidater发送了一轮RequestVote后，通过随机选举暂停（randomized election timeouts）避免选举分裂造成多个Candidater迟迟无法选出Leader。
2. Follower接收到Leader发出的AppendEntries后确认系统中存在一个Leader，重置定时器以避免发起更高Term的选举过程。
3. Leader发送完一轮AppendEntries后重置自己的定时器，稳定保持在自己的Term。

Candidater发送一轮RequestVote可以采用协程进行，当超过半数以上节点同意后，成为Leader，发起第一轮AppendEntries。当Candidater所在Term过低（5.1）或日志过于陈旧（5.2、5.4），无法成为Leader，改变状态为Follower，等待更合适的Candidater成为Leader.Follower会将每Term的那张选票投给第一个向它发送RequestVote的Candidater，之后同一Term的Candidater都再无法获得这个Folloer在该Term下的选票。

```
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
```
```
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
```
Leader向系统中群发AppendEntries RPC时，若没有需要同步的日志则仅发送心跳用于保活；若存在需要同步的日志则发送带日志的同步消息；若需要同步的日志已经被日志压缩进状态机快照中，则发送InstallSnapshot RPC用于副本同步日志。需要注意的是，AppendEntries中会携带prevLogIndex和prevLogTerm用于验证处在prevLogIndex及其之前的日志是否一直。副本节点根据关键点2中的日志匹配原则，通过AppendEntries中携带的信息确认是否与Leader日志在prevLogIndex及以前匹配。若不匹配着返回副本节点在prevLogTerm中获得的第一条日志处Index，Leader根据返回的信息调整NextIndex，更新下一次AppendEntries时应当向该副本发送的日志Index，最终使副本在[1,prevLogIndex]范围内和Leader日志相同，并将Leader日志中[prevLogIndex+1,lastLogIndex]部分写入副本日志记录中。若副本对AppendEntries RPC返回True，则认为副本日志记录达到了Leader发送AppendEntries所在的状态，更新mathIndex.同时副本会尽量将CommitIndex向Leader靠拢，Leader会在一半以上副本的mathIndex到达后更新CommitIndex.

```
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
```

```
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
```
“To eliminate problems like the one in Figure 8, Raft never commits log entries from previous terms by counting replicas. Only log entries from the leader’s current term are committed by counting replicas; once an entry from the current term has been committed in this way, then all prior entries are committed indirectly because of the Log Matching Property. There are some situations where a leader could safely conclude that an older log entry is committed (for example, if that entry is stored on every server), but Raft takes a more conservative approach for simplicity.”（为了消除类似图8中的问题，Raft从不通过计算副本来提交以前条目的日志。通过对副本进行计数，仅提交领导者当前任期的日志条目；一旦以这种方式提交了当前条目，则由于日志匹配属性，所有先前的条目都被间接提交。在某些情况下，领导者可以安全地断定一个旧的日志条目被提交了(例如，如果那个条目被存储在每个服务器上)，但是Raft为了简单起见采取了一种更保守的方法。）

```
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
```




当副本收到Leader发出的同步快照时，“Usually the snapshot will contain new information not already in the recipient’s log. In this case, the follower discards its entire log; it is all superseded by the snapshot and may possibly have uncommitted entries that conflict with the snapshot. If instead the follower receives a snapshot that describes a prefix of its log (due to retransmission or by mistake), then log entries covered by the snapshot are deleted but entries following the snapshot are still valid and must be retained”。（通常，快照会包含收件人日志中尚未包含的新信息。在这种情况下，跟随者丢弃它的整个日志；它全部被快照取代，并且可能具有与快照冲突的未提交条目。相反，如果跟随者接收到描述其日志前缀的快照(由于重传或错误)，则快照覆盖的日志条目被删除，但是快照之后的条目仍然有效，并且必须被保留。）

```
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
```

日志命令的提交过程由单独的协程实现。applyTicker会将lastApplied~commitIndex的日志提交到上层状态机用于实现命令。若存在已经被日志压缩的命令，则提交快照用于将状态机同步到SnapShopIndex之后。
```
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

```
节点的持久化需要保存的信息在Figure 2State中列出，当引入日志压缩快照后，压缩点处的信息SnapShopTerm、SnapShopIndex也应当一同持久化。

```
func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.SnapShopIndex)
	return w.Bytes()
}
```
