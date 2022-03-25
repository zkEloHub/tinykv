package raft

import "fmt"

var (
	InvalidLogIndexErr = fmt.Errorf("[RaftInternalError]: %s", "invalid log index")
)
