package shardkv

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
//
type Op struct {
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	return false
}

//
// additions to ShardKV state
//
type ShardKVImpl struct {
}

//
// initialize kv.impl.*
//
func (kv *ShardKV) InitImpl() {
}

//
// RPC handler for client Get requests
//
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	return nil
}

//
// RPC handler for client Put and Append requests
//
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	return nil
}

//
// Execute operation encoded in decided value v and update local state
//
func (kv *ShardKV) ApplyOp(v interface{}) {
}

//
// Add RPC handlers for any other RPCs you introduce
//
