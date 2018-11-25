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

import "sync"
import "labrpc"

import "math/rand"
import "time"

// import "bytes"
// import "encoding/gob"


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// State a Raft server must maintain.
	State           string

	CurrentTerm     int
	VotedFor        int
	log []LogEntry

	CommitIndex int
	LastApplied int

	NextIndex []int
	MatchIndex []int

	lastMessageTime int64//判断什么时候开始去看能不能竞选
	startHeatBeat       chan bool//是否开始发送心跳
}

type LogEntry struct {
	Command interface{}
	Term int
}

func (rf *Raft) becomeCandidate() {
	rf.State = "Candidate"
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.VotedFor = rf.me
}

func (rf *Raft) becomeLeader() {
	rf.State = "Leader"
}

func (rf *Raft) becomeFollower(term int, candidate int) {
	rf.State = "Follower"
	rf.CurrentTerm = term
	rf.VotedFor = candidate
	rf.lastMessageTime = getPresentMileTime()
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here.
	term = rf.CurrentTerm
	isLeader = (rf.State == "Leader")
	return term, isLeader
}

//
// save Raft's persistent State to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted State.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	if args.Term <= rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	} else {
		rf.becomeFollower(args.Term, args.CandidateId)
		reply.VoteGranted = true
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
	} else {
		reply.Success = true
		reply.Term = rf.CurrentTerm
		rf.mu.Lock()
		rf.VotedFor = args.LeaderId
		rf.State = "Follower"
		rf.lastMessageTime = getPresentMileTime()
		rf.mu.Unlock()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func getPresentMileTime() int64 {
	//带纳秒的时间戳
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// 心跳部分
func (rf *Raft) sendHeartBeat() int {
	timeout := 20
	var success_count int
	success_count = 0
	// 发送
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			c := make(chan bool, 1)

			var reply AppendEntriesReply
			var args AppendEntriesArgs
			args.LeaderId = rf.me
			args.Term = rf.CurrentTerm
			go func() { c <- rf.peers[i].Call("Raft.AppendEntries", args, &reply) }()
			select {
			case ok := <- c:
				if ok && reply.Success {
					success_count++
				}
			case <-time.After(time.Duration(timeout) * time.Millisecond):
				continue
			}
		}
	}
	return success_count
}

// 发送心跳
func (rf *Raft) sendAppendEntriesImpl() {
	//如果我是leader
	if rf.State == "Leader" {
		// 发送
		c := make(chan int, 1)
		go func() { c <- rf.sendHeartBeat() }()
		select {
		case count := <- c:
				// 如果发送成功 一半以上收到
			if count >= len(rf.peers)/2 {
				rf.mu.Lock()
				rf.lastMessageTime = getPresentMileTime()
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) sendLeaderHeartBeat() {
	timeout := 20
	for {
		select {
		case <-rf.startHeatBeat:
			rf.sendAppendEntriesImpl()
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			rf.sendAppendEntriesImpl()
		}
	}
}
// 心跳部分

// 选举部分
func (rf *Raft) sendRequestVoteAndCount() int {
	timeout := 20
	var done int
	done = 0
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			var args RequestVoteArgs
			args.Term = rf.CurrentTerm
			args.CandidateId = rf.me
			var reply RequestVoteReply

			c := make(chan bool, 1)
			go func(){c <- rf.sendRequestVote(i, args, &reply)}()
			select {
			case <- c:
				if reply.VoteGranted {
					done++
				}
			case <-time.After(time.Duration(timeout) * time.Millisecond):
				continue
			}
		}
	}
	return done
}

func (rf *Raft) election_one_round() bool {
	var timeout int64
	timeout = 100
	last := getPresentMileTime()
	success := false
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()
	done := 0
	for {
		// 找大家求选票
		c := make(chan int, 1)
		go func() { c <- rf.sendRequestVoteAndCount() }()
		select {
		case ok := <- c:
				// 如果发送成功 一半以上收到
			done = ok
			success = (done >= len(rf.peers)/2) && (rf.VotedFor == rf.me)
			if success {
				rf.mu.Lock()
				rf.becomeLeader()
				rf.mu.Unlock()
				rf.startHeatBeat <- true
			}
		}
		if (timeout+last < getPresentMileTime()) || (done >= len(rf.peers)/2) {
			return success
		}
	}
}

func (rf *Raft) election() {
	var result bool
	for {
		timeout := rand.Int63n(100) + 50
		rf.lastMessageTime = getPresentMileTime()
		for rf.lastMessageTime+timeout > getPresentMileTime() {
			select {
			// 超时了开始判定
			case <-time.After(time.Duration(timeout) * time.Millisecond):
			//如果没人竞选 
				if rf.lastMessageTime+timeout <= getPresentMileTime() {
					break
				} else {
					//别人去竞选了 
					rf.lastMessageTime = getPresentMileTime()
					timeout = rand.Int63n(100) + 50
					continue
				}
			}
		}

		// 竞选到成功
		result = false
		for !result {
			result = rf.election_one_round()
		}
	}
}
// 选举部分

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent State, and also initially holds the most
// recent saved State, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.State = "Follower"
	rf.startHeatBeat = make(chan bool)

	go rf.election()
	go rf.sendLeaderHeartBeat()

	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}