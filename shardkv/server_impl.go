package shardkv

import (
	"time"
	"umich.edu/eecs491/proj5/common"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
//
const (
	Get    = 0
	Put    = 1
	Append = 2
	Donate = 3
	Accept = 4
)

type Op struct {
	RequestId int
	Operation int
	Key       string
	Value     string
	ConfigNum int
	Shards    [common.NShards]int64
	Groups    map[int64][]string
	Database  map[string]string
	HandledId map[int]bool
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	op1 := v1.(Op)
	op2 := v2.(Op)
	return op1.RequestId == op2.RequestId
}

//
// additions to ShardKV state
//
type ShardKVImpl struct {
	ConfigNum int
	Shards    [common.NShards]int64
	Database  map[string]string
	HandledId map[int]bool
}

//
// initialize kv.impl.*
//
func (kv *ShardKV) InitImpl() {
	kv.impl.ConfigNum = 0
	kv.impl.Shards = [common.NShards]int64{}
	for i := 0; i < common.NShards; i++ {
		kv.impl.Shards[i] = int64(0)
	}
	kv.impl.Database = make(map[string]string)
	kv.impl.HandledId = make(map[int]bool)
}

//
// RPC handler for client Get requests
//
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	shard := common.Key2Shard(args.Key)
	kv.mu.Lock()
	if v, found := kv.impl.HandledId[args.Impl.RequestId]; found && v {
		kv.mu.Unlock()
		return nil
	}
	if kv.impl.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()
	op := Op{
		RequestId: args.Impl.RequestId,
		Operation: Get,
		Key:       args.Key,
	}
	kv.rsm.AddOp(op)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.impl.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}
	if value, ok := kv.impl.Database[args.Key]; ok {
		//log.Printf("%v Get on key %v value %v on replica %v of group %v", op.RequestId, op.Key, value, kv.me, kv.gid)
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

//
// RPC handler for client Put and Append requests
//
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	//log.Printf("%v Server %v of group %v received %v rpc with key %v value %v", args.Impl.RequestId, kv.me, kv.gid, args.Op, args.Key, args.Value)
	shard := common.Key2Shard(args.Key)
	kv.mu.Lock()
	if v, found := kv.impl.HandledId[args.Impl.RequestId]; found && v {
		kv.mu.Unlock()
		return nil
	}
	if kv.impl.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()
	op := Op{
		RequestId: args.Impl.RequestId,
		Operation: Put,
		Key:       args.Key,
		Value:     args.Value,
	}
	if args.Op == "Append" {
		op.Operation = Append
	}
	kv.rsm.AddOp(op)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.impl.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}
	reply.Err = OK
	return nil
}

//
// Execute operation encoded in decided value v and update local state
//
func (kv *ShardKV) ApplyOp(v interface{}) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := v.(Op)
	if _, isHandle := kv.impl.HandledId[op.RequestId]; isHandle {
		return
	} else {
		kv.impl.HandledId[op.RequestId] = true
		if op.Operation == Put {
			//log.Printf("%v Put on key %v value %v on replica %v of group %v", op.RequestId, op.Key, op.Value, kv.me, kv.gid)
			kv.impl.Database[op.Key] = op.Value
		} else if op.Operation == Append {
			if prev, ok := kv.impl.Database[op.Key]; ok {
				kv.impl.Database[op.Key] = prev + op.Value
			} else {
				kv.impl.Database[op.Key] = op.Value
			}
		} else if op.Operation == Donate {
			acceptors := make(map[int64][]int)
			for i := 0; i < common.NShards; i++ {
				if op.Shards[i] != kv.gid && kv.impl.Shards[i] == kv.gid {
					if v, ok := acceptors[op.Shards[i]]; ok {
						acceptors[op.Shards[i]] = append(v, i)
					} else {
						acceptors[op.Shards[i]] = []int{i}
					}
				}
			}
			kv.impl.Shards = op.Shards
			kv.impl.ConfigNum = op.ConfigNum
			for group, list := range acceptors {
				database := make(map[string]string)
				handledId := make(map[int]bool)
				for k, v := range kv.impl.Database {
					shard := common.Key2Shard(k)
					if common.Contains(list, shard) {
						database[k] = v
						delete(kv.impl.Database, k)
					}
				}
				for k, v := range kv.impl.HandledId {
					handledId[k] = v
				}
				kv.sendAcceptRPC(op.ConfigNum, op.Shards, database, handledId, op.Groups[group])
			}
		} else if op.Operation == Accept {
			kv.impl.Shards = op.Shards
			kv.impl.ConfigNum = op.ConfigNum
			for k, v := range op.Database {
				kv.impl.Database[k] = v
			}
			for k, v := range op.HandledId {
				kv.impl.HandledId[k] = v
			}
		}
	}
}

func (kv *ShardKV) sendAcceptRPC(configNum int, shards [common.NShards]int64, database map[string]string, handledId map[int]bool, servers []string) {
	requestId := int(common.Nrand())
	args := &common.AcceptDataArgs{
		RequestId: requestId,
		ConfigNum: configNum,
		Shards:    shards,
		Database:  database,
		HandledId: handledId,
	}
	var reply common.AcceptDataReply
	i := 0
	ok := common.Call(servers[i], "ShardKV.AcceptData", args, &reply)
	for !ok {
		i += 1
		i = i % len(servers)
		time.Sleep(10 * time.Millisecond)
		ok = common.Call(servers[i], "ShardKV.AcceptData", args, &reply)
	}
}

//
// Add RPC handlers for any other RPCs you introduce
//

func (kv *ShardKV) DonateData(args *common.DonateDataArgs, reply *common.DonateDataReply) error {
	op := Op{
		RequestId: args.RequestId,
		Operation: Donate,
		ConfigNum: args.ConfigNum,
		Shards:    args.Shards,
		Groups:    args.Groups,
	}
	kv.rsm.AddOp(op)
	return nil
}

func (kv *ShardKV) AcceptData(args *common.AcceptDataArgs, reply *common.AcceptDataReply) error {
	op := Op{
		RequestId: args.RequestId,
		Operation: Accept,
		ConfigNum: args.ConfigNum,
		Shards:    args.Shards,
		Database:  args.Database,
		HandledId: args.HandledId,
	}
	kv.rsm.AddOp(op)
	return nil
}
