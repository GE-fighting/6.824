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
	"log"
	"math/rand"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int
	votedFor    int
	role        State
	timeout     time.Time
	VoteNum     int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
	Term        int
	CandidateId int

	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntryArg struct {
	Term int
}

type AppendEntryReply struct {
	Term      int
	SyncState bool
}

// example RequestVote RPC handler.  处理发送过来的处理请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//候选者任期小于当前server当前任期，拒绝投票
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

	} else if rf.currentTerm < args.Term {
		//候选者任期大于当前server当前任期，投票，这个时候应该是不管有没有投过票，都要投票
		//未投过票
		reply.VoteGranted = true
		reply.Term = args.Term
		//更新当前server为Follower状态，更新任期和选举时间
		rf.role = Follower
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term

	} else {
		//只有是Follow才会同意投票，其他的只会给自己投吧
		if rf.role == Follower {
			//如果没有投过票，则发起投票
			if rf.votedFor != -1 {
				reply.VoteGranted = true
				reply.Term = args.Term
				//	当前当前服务选举时间
				rf.votedFor = args.CandidateId
			} else {
				reply.VoteGranted = false
				reply.Term = args.Term
			}
		} else {
			reply.VoteGranted = false
			reply.Term = args.Term
		}
	}
	//	当前当前服务选举时间
	rf.timeout = getElectionTimeout(rf.timeout)
	// Your code here (2A, 2B).
}
func (rf *Raft) AppendEntries(args *AppendEntryArg, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//所有的RPC都得判断一开始Term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.SyncState = false
		return
	} else {
		//更新共享数据，就得加锁
		rf.role = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.SyncState = true
	}
	rf.timeout = rf.timeout.Add(time.Duration(200+rand.Intn(300)) * time.Millisecond)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	if rf.role == Candidate {
		//从主线程传过来，保证在协程中修改数据，不会影响发送选举的RPC数据
		reply := &RequestVoteReply{}
		log.Printf("candidate-%d 调用 server-%d RequestVote \n", rf.me, server)
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			rf.mu.Lock()
			//如果得到选票，更新以获取的选票数量
			if reply.VoteGranted {
				log.Printf("candidate-%d 得到server-%d 的选票 \n", rf.me, server)
				rf.VoteNum++
				log.Printf("candidate-%d 的选票数为 %d \n", rf.me, rf.VoteNum)
			} else {
				//如果，没有得到选票，从返回来的结果，更新自己的任期，并又转化为Follower状态
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
					rf.timeout = rf.timeout.Add(time.Duration(200+rand.Intn(300)) * time.Millisecond)
				}
			}
			//如果目前还是Candidate时
			if rf.role == Candidate {
				if rf.VoteNum > len(rf.peers)/2 {
					log.Printf("candidate-%d 成为Leader ，开始并行发送心跳信息 \n", rf.me)
					rf.role = Leader
					beatArg := &AppendEntryArg{Term: rf.currentTerm}
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me {
							go rf.sendAppendEntries(i, beatArg)
						}
					}

				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArg) {
	reply := &AppendEntryReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		log.Printf("leader server-%d call AppendEntries RPC to server-%d failed\n", rf.me, server)
	} else {
		if reply.SyncState == false {
			log.Printf("leader-%d 发送心跳信息到 server-%d 返回false，leader更新为follower\n", rf.me, server)
			//转化为Follow角色后，要做的更新server操作
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.votedFor = -1
			rf.timeout = rf.timeout.Add(time.Duration(200+rand.Intn(300)) * time.Millisecond)
			rf.mu.Unlock()
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
	isLeader := true

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
func (rf *Raft) sustainedElection() {
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
	for rf.killed() == false {
		//1、当目前节点状态不是Leader时,判断是否选举
		if rf.role != Leader {
			log.Printf("\"Follower-%d，进入ticker阶段", rf.me)
			if time.Now().Before(rf.timeout) {
				time.Sleep(rf.timeout.Sub(time.Now()))
			} else {
				//	发起选举
				//1、更改本身的状态信息
				rf.mu.Lock()
				rf.currentTerm++
				rf.role = Candidate
				rf.votedFor = rf.me
				rf.timeout = getElectionTimeout(rf.timeout)
				rf.mu.Unlock()
				log.Printf("Follower-%d，达到选举超时点，转成Candidate,发起选举，任期为%d \n", rf.me, rf.currentTerm)
				//	2、向其他的server发起投票流程
				voteArgs := &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go rf.sendRequestVote(i, voteArgs)
					}
				}
			}

		}

	}
}

func (rf *Raft) sustainedSendHeartBeat() {
	for rf.killed() == false {
		if rf.role == Leader {
			//	每100ms向其他server发送心跳信息
			arg := &AppendEntryArg{Term: rf.currentTerm}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.sendAppendEntries(i, arg)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.timeout = getElectionTimeout(time.Now())
	log.Printf("创建follower-%d,选举时间-%v\n", me, rf.timeout)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.sustainedElection()
	go rf.sustainedSendHeartBeat()
	return rf
}

func getElectionTimeout(original time.Time) time.Time {
	return original.Add(time.Duration(200+rand.Intn(300)) * time.Millisecond)
}
