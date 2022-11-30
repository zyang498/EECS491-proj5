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
	for {
		config := ck.sm.Query(-1)
		shard := common.Key2Shard(key)
		servers := config.Groups[config.Shards[shard]]
		args := &GetArgs{
			Key: key,
			Impl: GetArgsImpl{
				RequestId: requestId,
				ConfigNum: config.Num,
			},
		}
		var reply GetReply
		for i := 0; i < len(servers); i++ {
			ok := common.Call(servers[i], "ShardKV.Get", args, &reply)
			if ok {
				if reply.Err == OK {
					return reply.Value
				} else if reply.Err == ErrNoKey {
					//log.Printf("%v Get RPC to server %v with key %v but no key", requestId, servers[i], key)
					return ""
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
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
	for {
		config := ck.sm.Query(-1)
		shard := common.Key2Shard(key)
		servers := config.Groups[config.Shards[shard]]
		args := &PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
			Impl: PutAppendArgsImpl{
				RequestId: requestId,
				ConfigNum: config.Num,
			},
		}
		var reply PutAppendReply
		for i := 0; i < len(servers); i++ {
			//log.Printf("%v Sending %v rpc to group %v servers %v with key %v value %v", args.Impl.RequestId, op, config.Shards[shard], servers[i], key, value)
			ok := common.Call(servers[i], "ShardKV.PutAppend", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
