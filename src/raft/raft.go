package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type HeartBeatLogType int

const (
	HeartBeatLog HeartBeatLogType = iota
	AppendEntriesLog
)

// Log entry
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int                 // 当前任期
	votedFor    int                 //已经投给了谁
	role        State               //当前服务的角色，Leader / Follower / Candidater
	timeout     time.Time           //选举超时的时间点
	VoteNum     int                 //拿到的选票数量
	logs        []*LogEntry         //日志条目切片
	commitIndex int                 //提交日志条目索引
	lastApplied int                 //上一个运行的日志条目索引
	nextIndex   []int               //对于每个服务器，下一个要发送到该服务器的日志条目的索引，选举成功的时候初始化
	matchIndex  []int               //对于每台服务器，已知在服务器上复制成功的最高日志条目的索引，选举成功的时候初始化
	leaderId    int                 //节点所知的leaderId
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//状态机
	applyCond *sync.Cond
	applyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.role == Leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

// get log last index
func (rf *Raft) getLogLastIndex() int {
	return len(rf.logs) - 1
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type RequestVoteResult struct {
	peerId int
	resp   *RequestVoteReply
}

// 心跳或日志追加的参数
type AppendEntryArg struct {
	Term              int         // 领导者任期
	LeaderId          int         // 领导者ID
	PrevLogIndex      int         // 新日志前的索引 和任期一起做一致性检查
	PrevLogTerm       int         // 新日志前的任期
	Entries           []*LogEntry // 日志条目
	LeaderCommitIndex int         //领导者已经提交的日志索引
	HeartBeatType     HeartBeatLogType
}

type AppendEntryReply struct {
	Term      int
	SyncState bool
	Success   bool // Follower 日志中匹配preLogIndex 和 preLogTerm 返回true; 否则false
}

//rules : 1、如果AppendEntryArg中Term 小于 follower中的currentTerm，返回false
//		  2、

// example RequestVote RPC handler.  处理发送过来的处理请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.timeout = getElectionTime(rf.timeout)
	DPrintf("node-%d 收到 candidate-%d vote request，时间是-%v\n", rf.me, args.CandidateId, time.Now())
	DPrintf("node-%d 更新选举时间为 %v\n", rf.me, rf.timeout)

	//	当前当前服务选举时间
	//候选者任期小于当前server当前任期，拒绝投票
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		//更新当前server为Follower状态，更新任期
		rf.role = Follower
		rf.currentTerm = args.Term
		//需要比较一条最新的日志情况再决定要不要投票
		rf.votedFor = -1
		rf.leaderId = -1
		rf.persist()
	}
	//避免重复投票,投过一次就不会再投了
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//拿到自己最新的日志index和term
		index, term := rf.getLogLastIndex(), rf.logs[rf.getLogLastIndex()].Term
		//candidate最后一条任期更大 或者任期一样日志更长
		if args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= index) {
			rf.role = Follower
			rf.votedFor = args.CandidateId
			rf.leaderId = args.CandidateId
			reply.VoteGranted = true
			rf.persist()
		}

	} else {
		DPrintf("node-%d 跟候选者node-%d任期一样，但是已经投了另一位候选人node-%d，所以这里拒绝选举\n", rf.me, args.CandidateId, rf.votedFor)
	}

	// Your code here (2A, 2B).
}

// 处理心跳请求
func (rf *Raft) AppendEntries(args *AppendEntryArg, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//初始化数据
	reply.Success = false
	reply.Term = rf.currentTerm
	//所有的RPC都得判断一开始Term,拒绝旧leader请求
	if args.Term < rf.currentTerm {
		return
	}
	//收到任期更大的心跳信息，转为这个任期的follower，即 Follower,Leader -> follower
	//if args.Term > rf.currentTerm {
	//	//更新共享数据，就得加锁
	//	rf.role = Follower
	//	rf.currentTerm = args.Term
	//	rf.persist()
	//}
	//心跳消息的任期大于等于自己的
	rf.leaderId = args.LeaderId
	rf.votedFor = args.LeaderId
	rf.role = Follower
	rf.currentTerm = args.Term
	rf.timeout = getElectionTime(rf.timeout)
	rf.persist()
	DPrintf("time-%v,node-%d 收到了leader-%d的心跳消息，选举时间是-%v\n", time.Now(), rf.me, args.LeaderId, rf.timeout)
	//还缺少前面的日志或者前一条日志匹配不上
	if args.PrevLogIndex > rf.getLogLastIndex() || rf.logs[rf.getLogLastIndex()].Term != args.PrevLogTerm {
		return
	}
	//args.PrevLogIndex<= rf.lastLogIndex，有可能发生截断的情况，即当前节点的日志要比leader的还要长
	if rf.getLogLastIndex() > args.PrevLogIndex {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}
	//似乎不管preLogIndex及以前的日志条目了
	rf.logs = append(rf.logs, args.Entries...)
	rf.persist()
	//leader通过心跳信息告诉当前节点已经已经commit到哪了
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = args.LeaderCommitIndex
		//同时还要与当前日志长度比较啊
		if rf.getLogLastIndex() < rf.commitIndex {
			rf.commitIndex = rf.getLogLastIndex()
		}
	}
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(args *RequestVoteArgs) (int, int) {
	//定义投票结果chan
	results := make(chan *RequestVoteResult, len(rf.peers)-1)
	//向其他服务发送投票消息
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int, args *RequestVoteArgs) {
				reply := &RequestVoteReply{}
				ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
				if ok {
					results <- &RequestVoteResult{
						peerId: server,
						resp:   reply,
					}
				} else {
					results <- &RequestVoteResult{
						peerId: server,
						resp:   nil,
					}
				}
			}(i, args)

		}
	}
	//定义当前最大的任期
	maxTerm := rf.currentTerm
	//同意选票数
	voteGranted := 1
	//总投票数
	totalVote := 1
	//取出chan中的投票结果做判断
	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case vote := <-results:
			totalVote++
			if vote.resp != nil {
				if vote.resp.VoteGranted {
					voteGranted++
				}
				//如果发现其他服务存在更大的任期，则回退为follower
				if vote.resp.Term > maxTerm {
					maxTerm = vote.resp.Term
				}
			}
		}
		//已经有一半的服务同意 或 所以的节点都收到返回消息后
		if voteGranted > len(rf.peers)/2 || totalVote == len(rf.peers) {
			return maxTerm, voteGranted
		}
	}
	return maxTerm, voteGranted
}

// leader 节点向其他服务发送心跳消息
func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArg) {
	reply := &AppendEntryReply{}
	DPrintf("进入发送到node-%d 流程\n", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("node-%d 返回值了\n", server)
	if !ok {
		DPrintf("leader server-%d call AppendEntries RPC to server-%d failed\n", rf.me, server)
		return
	}
	//如果term变了，表示该结点不再是leader，什么也不做;绝了，确实是这样的
	if rf.currentTerm != args.Term {
		return
	}
	rf.mu.Lock()
	rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("leader-%d 发送心跳信息到 server-%d 返回false，leader更新为follower\n", rf.me, server)
		//转化为Follow角色后，更新服务状态，并持久化
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.leaderId = -1
		rf.timeout = getElectionTime(rf.timeout)
		rf.persist()
		return
	}
	if reply.Success {
		//1、更新nextIndex[]和matchIndex[]
		rf.nextIndex[server] += len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		//提交到哪个位置需要根据中位数来判断，中位数表示过半提交的日志位置，
		//每次想要提交日志向各结点发送的日志并不完全一样，不能光靠是否发送成功来判断哪些日志应该提交
		//1、初始化已经传到给个服务结点上的日志的matchIndex切片
		matchIndexSlice := make([]int, len(rf.peers))
		for server, matchIndex := range rf.matchIndex {
			matchIndexSlice[server] = matchIndex
		}
		//matchIndex 从小到大排序
		sort.Slice(matchIndexSlice, func(i, j int) bool {
			return matchIndexSlice[i] < matchIndexSlice[j]
		})
		//取matchIndex的中位数来表示提交最终的commitIndex
		newCommitIndex := matchIndexSlice[len(rf.peers)/2]
		//提交日志
		if newCommitIndex > rf.commitIndex && args.Term == rf.logs[newCommitIndex].Term {
			DPrintf("leader node-%d commit logEntry-%d\n", rf.me, newCommitIndex)
			rf.commitIndex = newCommitIndex
		}
	} else {
		// 返回false目前只有一种可能，就是目标服务Log缺少，所以要回退nextIndex[server]
		rf.nextIndex[server] -= 1
		//注意临界条件
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	if rf.role != Leader {
		return index, term, false
	}
	isLeader := true
	//1、将该命令添加到本地的日志条目中
	term = rf.currentTerm
	index = rf.getLogLastIndex()
	entry := &LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.logs = append(rf.logs, entry)
	//2、并行发送RPC将日志复制到其他服务上
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			nextIndex := rf.nextIndex[i]
			args := &AppendEntryArg{Term: rf.currentTerm, PrevLogIndex: rf.logs[nextIndex-1].Index,
				PrevLogTerm: rf.logs[nextIndex-1].Term, LeaderId: rf.me, Entries: rf.logs[nextIndex:]}
			//在协程里面处理后续逻辑
			go rf.sendAppendEntries(i, args)
		}
	}
	// Your code here (2B).
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
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
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
	for rf.killed() == false {
		time.Sleep(1 * time.Millisecond)
		//1、当目前节点状态不是Leader时,判断是否选举
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role == Leader {
				return
			}
			if time.Now().Before(rf.timeout) {
				return
			}
			//	发起选举
			//1、更改本身的状态信息
			rf.currentTerm++
			rf.role = Candidate
			rf.votedFor = rf.me
			DPrintf("-----------------------------Follower-%d，达到选举超时点%v，转成Candidate,发起选举，任期为%d \n", rf.me, rf.timeout, rf.currentTerm)
			rf.timeout = getElectionTime(rf.timeout)
			rf.persist()
			rf.mu.Unlock()

			//	2、向其他的server发起投票流程
			voteArgs := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLogLastIndex(),
				LastLogTerm:  rf.logs[rf.getLogLastIndex()].Term,
			}
			maxTerm, agreeNum := rf.sendRequestVote(voteArgs)
			//如果节点角色不是candidate
			if rf.role != Candidate {
				return
			}
			rf.mu.Lock()
			if maxTerm > rf.currentTerm {
				rf.role = Follower
				rf.currentTerm = maxTerm
				rf.votedFor = -1
				rf.leaderId = -1
				rf.persist()
			} else if agreeNum > len(rf.peers)/2 {
				rf.role = Leader
				rf.leaderId = rf.me
				DPrintf("node-%d 成功leader\n", rf.me)
				//马上发一条心跳消息
				rf.HeartBeat()
			}

		}()

	}
}

// leader节点 持续发送心跳消息
func (rf *Raft) loopHeartBeat() {
	for rf.killed() == false {
		time.Sleep(150 * time.Millisecond)
		if rf.role == Leader {
			DPrintf("node-%d 现在是leader了，发送心跳消息，任期是%d,时间是%v\n", rf.me, rf.currentTerm, time.Now())
			rf.HeartBeat()
			//	每100ms向其他server发送心跳信息
			//for i := 0; i < len(rf.peers); i++ {
			//	//对自己发送心跳消息
			//	if i == rf.me {
			//		rf.matchIndex[i] = rf.getLogLastIndex()
			//		rf.nextIndex[i] = rf.matchIndex[i] + 1
			//		continue
			//	}
			//	//对其他节点发送心跳消息
			//	//记录所发送日志的前一行日志
			//	preIndex := rf.matchIndex[i]
			//	//构造心跳请求参数
			//	arg := AppendEntryArg{
			//		Term:              rf.currentTerm,
			//		LeaderId:          rf.me,
			//		PrevLogIndex:      preIndex,
			//		PrevLogTerm:       rf.logs[preIndex].Term,
			//		HeartBeatType:     HeartBeatLog,
			//		LeaderCommitIndex: rf.commitIndex,
			//	}
			//	//确认Leader所有日志都被同步到了其他节点上
			//	//拿到最新日志索引
			//	logLastIndex := rf.getLogLastIndex()
			//	if rf.matchIndex[i] < logLastIndex {
			//		//说明还有日志没有同步过去，更改心跳RPC的参数类型
			//		arg.HeartBeatType = AppendEntriesLog
			//		entries := make([]LogEntry, 0)
			//		//因为此时没有加锁，担心有新日志写入，必须保证每个节点复制的最后一条日志一样才能起到过半提交的效果
			//		arg.Entries = append(entries, rf.logs[rf.nextIndex[i]:logLastIndex+1]...)
			//	}
			//	go rf.sendAppendEntries(i, &arg)
			//}

		}
	}
}

func (rf *Raft) HeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		//对自己发送心跳消息
		if i == rf.me {
			rf.matchIndex[i] = rf.getLogLastIndex()
			rf.nextIndex[i] = rf.matchIndex[i] + 1
			continue
		}
		//对其他节点发送心跳消息
		//记录所发送日志的前一行日志
		preIndex := rf.matchIndex[i]
		//构造心跳请求参数
		arg := AppendEntryArg{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			PrevLogIndex:      preIndex,
			PrevLogTerm:       rf.logs[preIndex].Term,
			HeartBeatType:     HeartBeatLog,
			LeaderCommitIndex: rf.commitIndex,
		}
		//确认Leader所有日志都被同步到了其他节点上
		//拿到最新日志索引
		logLastIndex := rf.getLogLastIndex()
		if rf.matchIndex[i] < logLastIndex {
			//说明还有日志没有同步过去，更改心跳RPC的参数类型
			arg.HeartBeatType = AppendEntriesLog
			entries := make([]*LogEntry, 0)
			//因为此时没有加锁，担心有新日志写入，必须保证每个节点复制的最后一条日志一样才能起到过半提交的效果
			arg.Entries = append(entries, rf.logs[rf.nextIndex[i]:logLastIndex+1]...)
		}
		DPrintf("leader-%d 向 node-%d 发送消息\n", rf.me, i)
		go rf.sendAppendEntries(i, &arg)
	}
}

func (rf *Raft) loopApplyLog(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		//定义将要发送到applych中的数据
		applyMsgs := make([]ApplyMsg, 0)
		if rf.lastApplied >= rf.commitIndex {
			return
		}
		rf.mu.Lock()
		//如果最新的被应用到状态机上的日志索引小于已提交的日志索引
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsgs = append(applyMsgs, ApplyMsg{
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
				CommandValid: true})
		}
		rf.mu.Unlock()
		go func() {
			for i := 0; i < len(applyMsgs); i++ {
				DPrintf("node-%d apply log index-%d and upload to application\n", rf.me, rf.lastApplied)
				applyCh <- applyMsgs[i]
			}
		}()
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      -1,

		leaderId:    -1,
		currentTerm: 0,
		votedFor:    -1,
		role:        Follower,
		timeout:     getElectionTime(time.Now()),

		commitIndex: 0,
		lastApplied: 0,
		applyChan:   applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCond = sync.NewCond(&rf.mu)
	DPrintf("创建follower-%d,选举时间-%v\n", me, rf.timeout)
	//初始化log
	rf.logs = make([]*LogEntry, 0)
	//init index state
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}
	rf.logs = append(rf.logs, &LogEntry{
		Index: 0,
		Term:  0,
	})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.loopHeartBeat()
	go rf.loopApplyLog(applyCh)
	return rf
}

func getElectionTime(original time.Time) time.Time {
	return original.Add(time.Duration(200+rand.Intn(150)) * time.Millisecond)
}
