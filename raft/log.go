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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	ret := &RaftLog{
		storage:   storage,
		committed: 0,
		applied:   0,
		// snapshot
	}
	storageIdx, err := storage.LastIndex()
	if err != nil {
		log.Errorf("[newLog] get storage's LastIndex failed, %v\n", err)
		return nil
	}
	ret.stabled = storageIdx
	return ret
}

// appendEntries used by leader
func (l *RaftLog) appendEntries(entries []*pb.Entry) {
	for _, entry := range entries {
		l.entries = append(l.entries, *entry)
	}
	// TODO: when to stable entries ?
}

// appendEntrySafely append one entry, and will handle entry conflicts (used by follower & candidate)
func (l *RaftLog) appendEntrySafely(entry *pb.Entry) (uint64, bool) {
	wantIdx := l.LastIndex() + 1
	if entry.Index == wantIdx {
		l.entries = append(l.entries, *entry)
		return 0, true
	}
	// invalid
	if entry.Index > wantIdx {
		return wantIdx, false
	}
	// conflicts, remove it and all entries that follower it
	if entry.Index <= l.stabled {
		term, err := l.storage.Term(entry.Index)
		if err == nil {
			if term != entry.Term {
				// TODO: delete truly from storage.
				// 1. "delete" from storage (set stabled index)
				// 2. delete all entries after that
				l.stabled = entry.Index
				l.entries = l.entries[:0]
			}
		}
		return 0, true
	}
	checkIdx := entry.Index - l.stabled - 1
	if l.entries[checkIdx].Term != entry.Term {
		l.entries = l.entries[:checkIdx+1]
		l.entries[checkIdx] = *entry
	}
	return 0, true
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	firstIdx := l.entries[0].Index
	if firstIdx > l.stabled {
		return l.entries
	}
	startIdx := l.stabled + 1 - firstIdx
	return l.entries[startIdx:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	// Your Code Here (2A).
	if l.committed == l.applied {
		return nil
	}

	retEntries := make([]pb.Entry, 0)
	// stabled entries
	if l.applied < l.stabled {
		rightEdge := min(l.committed, l.stabled)
		entries, err := l.storage.Entries(l.applied+1, rightEdge+1)
		if err == nil {
			retEntries = append(retEntries, entries...)
		}
	}
	// not stabled entries
	if l.committed > l.stabled && len(l.entries) > 0 {
		locateIdx := max(l.stabled+1, l.applied)
		leftIdx, found := l.locateEntryIndex(locateIdx)
		if !found {
			return retEntries
		}
		rightIdx, found := l.locateEntryIndex(l.committed)
		if !found {
			return retEntries
		}
		for i := leftIdx; i <= rightIdx; i++ {
			retEntries = append(retEntries, l.entries[i])
		}
	}
	return retEntries
}

// followerEnts return all entries behind nextIdx (include nextIdx it self)
func (l *RaftLog) getNextEnts(nextIdx uint64) []*pb.Entry {
	retEntries := make([]*pb.Entry, 0)
	if nextIdx <= l.stabled {
		entries, err := l.storage.Entries(nextIdx, l.stabled+1)
		if err == nil {
			for _, entry := range entries {
				copyEntry := entry
				retEntries = append(retEntries, &copyEntry)
			}
		}
	}
	locateIdx := max(nextIdx, l.stabled+1)
	i, found := l.locateEntryIndex(locateIdx)
	if !found {
		return retEntries
	}
	for ; i < uint64(len(l.entries)); i++ {
		retEntries = append(retEntries, &l.entries[i])
	}
	return retEntries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		idx, err := l.storage.LastIndex()
		if err != nil {
			return 0
		}
		return idx
	}
	entryLen := len(l.entries)
	return l.entries[entryLen-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i <= l.stabled {
		return l.storage.Term(i)
	}

	entryI, found := l.locateEntryIndex(i)
	if !found {
		return 0, InvalidLogIndexErr
	}
	return l.entries[entryI].Term, nil
}

// locateEntryIndex return the index of entry which Index is idx; bool means isFound
func (l *RaftLog) locateEntryIndex(idx uint64) (uint64, bool) {
	if len(l.entries) == 0 {
		return 0, false
	}
	firstIdx := l.entries[0].Index
	if idx < firstIdx {
		// should be noted
		return 0, true
	}
	return idx - firstIdx, true
}
