package shardkv

import (
	"time"
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
	requestId := int(common.Nrand())
	config := ck.sm.Query(-1)
	shard := common.Key2Shard(key)
	servers := config.Groups[config.Shards[shard]]
	args := &GetArgs{
		Key: key,
		Impl: GetArgsImpl{
			RequestId: requestId,
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
			time.Sleep(5 * time.Millisecond)
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
	requestId := int(common.Nrand())
	config := ck.sm.Query(-1)
	shard := common.Key2Shard(key)
	servers := config.Groups[config.Shards[shard]]
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Impl: PutAppendArgsImpl{
			RequestId: requestId,
		},
	}
	var reply PutAppendReply
	i := 0
	for {
		//log.Printf("%v Sending %v rpc to group %v servers %v with key %v value %v", args.Impl.RequestId, op, config.Shards[shard], servers[i], key, value)
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
			time.Sleep(5 * time.Millisecond)
			i += 1
			i = i % len(servers)
		}
	}
}
