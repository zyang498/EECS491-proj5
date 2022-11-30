package shardkv

import (
	"umich.edu/eecs491/proj5/common"
)

//
// additions to Clerk state
//
type ClerkImpl struct {
	ReceivedId map[int]bool
}

//
// initialize ck.impl.*
//
func (ck *Clerk) InitImpl() {
	ck.impl.ReceivedId = make(map[int]bool)
}

//
// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	config := ck.sm.Query(-1)
	shard := common.Key2Shard(key)
	servers := config.Groups[config.Shards[shard]]
	args := &GetArgs{
		Key: key,
		Impl: GetArgsImpl{
			RequestId: int(common.Nrand()),
		},
	}
	var reply GetReply
	i := 0
	for {
		ok := common.Call(servers[i], "ShardKV.Get", args, &reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			} else {
				config = ck.sm.Query(-1)
				shard = common.Key2Shard(key)
				servers = config.Groups[config.Shards[shard]]
				i = 0
			}
		} else {
			i += 1
			i = i % len(servers)
		}
	}
}

//
// send a Put or Append request.
// keep retrying forever until success.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	config := ck.sm.Query(-1)
	shard := common.Key2Shard(key)
	servers := config.Groups[config.Shards[shard]]
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Impl: PutAppendArgsImpl{
			RequestId: int(common.Nrand()),
		},
	}
	var reply PutAppendReply
	i := 0
	for {
		ok := common.Call(servers[i], "ShardKV.PutAppend", args, &reply)
		if ok {
			if reply.Err == OK {
				return
			} else if reply.Err == ErrWrongGroup {
				config = ck.sm.Query(-1)
				shard = common.Key2Shard(key)
				servers = config.Groups[config.Shards[shard]]
				i = 0
			}
		} else {
			i += 1
			i = i % len(servers)
		}
	}
}
