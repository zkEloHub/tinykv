// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// **Match: index where leader and follower agree. Next: index where next entry to send**
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	// key: id; used in request vote.
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// init raft with config.
	rLog := newLog(c.Storage)
	rLog.applied = c.Applied
	raft := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          rLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            map[uint64]bool{},
		heartbeatTimeout: c.HeartbeatTick,
		heartbeatElapsed: 0,
		electionTimeout:  c.ElectionTick,
		electionElapsed:  0,
	}
	for _, peerID := range c.peers {
		// initialed when become leader
		raft.Prs[peerID] = nil
	}

	return raft
}

func (r *Raft) newBaseMsg(to uint64) (*pb.Message, error) {
	msg := &pb.Message{
		To:   to,
		From: r.id,
		Term: r.Term,
	}
	LogIdx := r.RaftLog.LastIndex()
	msg.Index = LogIdx
	msg.Commit = r.RaftLog.committed

	//var err error
	msg.LogTerm, _ = r.RaftLog.Term(LogIdx)
	//if err != nil {
	//	return nil, err
	//}
	return msg, nil
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	msg, err := r.newBaseMsg(to)
	if err != nil {
		log.Errorf("[sendAppend] newBaseMsg failed, %s", err.Error())
		return false
	}
	msg.MsgType = pb.MessageType_MsgAppend
	msg.Entries = r.RaftLog.getNextEnts(r.Prs[to].Next)

	// TODO: [note] not actually send...
	r.msgs = append(r.msgs, *msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg, err := r.newBaseMsg(to)
	if err != nil {
		log.Errorf("[sendHeartbeat] newBaseMsg failed, %s", err.Error())
		return
	}
	msg.MsgType = pb.MessageType_MsgHeartbeat
	progress := r.Prs[to]
	if progress.Match > 0 {
		msg.Entries = r.RaftLog.getNextEnts(progress.Next)
	}

	r.msgs = append(r.msgs, *msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed = (r.heartbeatElapsed + 1) % r.heartbeatTimeout
	r.electionElapsed = (r.electionElapsed + 1) % r.electionTimeout
	if r.heartbeatElapsed == 0 {
		r.heartbeatElapsedHandler()
	}
	if r.electionElapsed == 0 {
		r.electionElapsedHandler()
	}
}

// heartbeat timeout handler.
func (r *Raft) heartbeatElapsedHandler() {
	if r.State != StateLeader {
		return
	}
	for peerID := range r.Prs {
		// TODO: parallel
		if peerID == r.id {
			continue
		}
		r.sendHeartbeat(peerID)
	}
}

// election timeout handler.
func (r *Raft) electionElapsedHandler() {
	// vote for myself.
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	r.sendRequestVoteRequests()
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	// if == 0, do not update r.Lead
	if lead == 0 {
		r.Vote = 0
		return
	}
	r.Lead = lead
	r.Vote = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// change state; term++; vote for itself
	r.State = StateCandidate
	r.Term++
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Vote = r.id
}

// sendRequestVoteRequests send request vote to all peers
func (r *Raft) sendRequestVoteRequests() {
	// TODO: in parallel
	for peerID := range r.Prs {
		if peerID == r.id {
			continue
		}
		msg, err := r.newBaseMsg(peerID)
		if err != nil {
			log.Errorf("[sendRequestVoteRequests] newBaseMsg failed, %s", err.Error())
			return
		}
		msg.MsgType = pb.MessageType_MsgRequestVote

		r.msgs = append(r.msgs, *msg)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateLeader {
		return
	}

	r.State = StateLeader
	for peerID := range r.Prs {
		r.Prs[peerID] = &Progress{
			Match: 0,
		}
	}

	// issues heartbeat in parallel (the first noop entry)
	for peerID := range r.Prs {
		// TODO: in parallel
		if peerID == r.id {
			continue
		}
		r.sendHeartbeat(peerID)
	}
}

// Step the entrance of **handle message**, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.StepFollower(m)
	case StateCandidate:
		return r.StepCandidate(m)
	case StateLeader:
		return r.StepLeader(m)
	}
	return nil
}

func (r *Raft) StepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup: // local: start election or after election timeout
		r.becomeCandidate()
		r.sendRequestVoteRequests()
	case pb.MessageType_MsgAppend: // append log entries
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote: // request vote
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat: // heartbeat from leader.
	default:
	}
	return nil
}

func (r *Raft) StepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote: // request vote from other candidate...
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: // request vote resp(may become leader)
		r.handleRequestVotesResp(m)
	case pb.MessageType_MsgHeartbeat: // heartbeat from leader(convert to follower)
	case pb.MessageType_MsgAppend: // append from leader(convert to follower)
		r.handleAppendEntries(m)
	default:
	}
	return nil
}

func (r *Raft) StepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat: // local: send heartbeat to followers
	case pb.MessageType_MsgPropose: // local: append data to leader's log entries
		r.RaftLog.appendEntries(m.Entries)
	case pb.MessageType_MsgAppendResponse: // append log entries resp
	case pb.MessageType_MsgHeartbeatResponse: // heartbeat resp
	case pb.MessageType_MsgTransferLeader: // leadership transfer
	case pb.MessageType_MsgRequestVote: // request vote from candidate
		r.handleRequestVote(m)
	default:
	}
	return nil
}

// handleAppendEntriesResp handle AppendEntries response from follower/candidate
func (r *Raft) handleAppendEntriesResp(m pb.Message) {

}

// handleHeartbeatResp handle heat beat response from follower/candidate
func (r *Raft) handleHeartbeatResp(m pb.Message) {

}

// handleAppendEntries handle AppendEntries RPC request from leader
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

}

// handleHeartbeat handle Heartbeat RPC request from leader
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// 1. check msg
	// 2. maybe transform to follower

}

// handleRequestVote handle request vote from candidate
func (r *Raft) handleRequestVote(m pb.Message) {
	newRequestVoteRespFunc := func() *pb.Message {
		msg, err := r.newBaseMsg(m.From)
		if err != nil {
			log.Errorf("[handleRequestVote] newBaseMsg failed, %s", err.Error())
			return nil
		}
		msg.MsgType = pb.MessageType_MsgRequestVoteResponse
		return msg
	}
	rejectFunc := func() {
		msg := newRequestVoteRespFunc()
		msg.Reject = true
		r.msgs = append(r.msgs, *msg)
	}
	supportFunc := func() {
		msg := newRequestVoteRespFunc()
		msg.Reject = false
		r.msgs = append(r.msgs, *msg)
		r.Vote = m.From
	}
	// 1. return false if m.term < current term
	// 2. reject it if current.vote is not null && not candidateID
	// 3. reject it if current if more 'up-to-date'

	// case-1
	if m.Term < r.Term {
		rejectFunc()
		return
	}
	// check term.
	if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
	}

	// case-2
	if r.Vote != 0 && r.Vote != m.From {
		rejectFunc()
		return
	}

	// case-3
	currentLogIdx := r.RaftLog.LastIndex()
	currentLogTerm, _ := r.RaftLog.Term(currentLogIdx)
	//if err != nil {
	//	log.Errorf("[handleRequestVote] Term() failed, %s", err.Error())
	//	return
	//}
	if m.LogTerm < currentLogTerm {
		rejectFunc()
		return
	}
	// more 'up-to-date'
	// 1.1 compare log term.
	if m.LogTerm > currentLogTerm {
		supportFunc()
		return
	}
	if m.LogTerm < currentLogTerm {
		rejectFunc()
		return
	}

	// 1.2 compare log index.
	if m.Index < currentLogIdx {
		rejectFunc()
		return
	}
	supportFunc()
	return
}

func (r *Raft) handleRequestVotesResp(m pb.Message) {
	if m.Reject == true {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, 0)
		}
		return
	}
	r.votes[m.From] = true
	if len(r.votes) > len(r.Prs)>>1 {
		r.becomeLeader()
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
