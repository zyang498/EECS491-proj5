package shardkv

import "umich.edu/eecs491/proj5/common"

// Field names must start with capital letters,
// otherwise RPC will break.

//
// additional state to include in arguments to PutAppend RPC.
//
type PutAppendArgsImpl struct {
	RequestId int
}

//
// additional state to include in arguments to Get RPC.
//
type GetArgsImpl struct {
	RequestId int
}

//
// for new RPCs that you add, declare types for arguments and reply
//

type AcceptDataArgs struct {
	RequestId int
	ConfigNum int
	Shards    [common.NShards]int64
	Database  map[string]string
	HandledId map[int]bool
}

type AcceptDataReply struct {
}
