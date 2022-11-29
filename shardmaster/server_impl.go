package shardmaster

import (
	"log"
	"umich.edu/eecs491/proj5/common"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
//
const (
	Join  = 0
	Leave = 1
	Move  = 2
	Query = 3
)

type Op struct {
	Operation int
	GID       int64
	Servers   []string
	Shard     int
	ConfigNum int
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	return v1 == v2
}

//
// additions to ShardMaster state
//
type ShardMasterImpl struct {
	ShardDistribution map[int64]int // group -> number of assigned shards
}

//
// initialize sm.impl.*
//
func (sm *ShardMaster) InitImpl() {
	sm.impl.ShardDistribution = make(map[int64]int)
	sm.impl.ShardDistribution[0] = common.NShards
}

func (sm *ShardMaster) getLatestConfig() Config {
	var config Config
	for i := 0; i < len(sm.configs); i++ {
		if i == len(sm.configs)-1 {
			config = sm.configs[i]
		}
	}
	return config
}

//
// RPC handlers for Join, Leave, Move, and Query RPCs
//
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	op := Op{
		Operation: Join,
		GID:       args.GID,
		Servers:   args.Servers,
		Shard:     0,
		ConfigNum: 0,
	}
	sm.rsm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	op := Op{
		Operation: Leave,
		GID:       args.GID,
		Servers:   nil,
		Shard:     0,
		ConfigNum: 0,
	}
	sm.rsm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	op := Op{
		Operation: Move,
		GID:       args.GID,
		Servers:   nil,
		Shard:     args.Shard,
		ConfigNum: 0,
	}
	sm.rsm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	op := Op{
		Operation: Query,
		GID:       0,
		Servers:   nil,
		Shard:     0,
		ConfigNum: args.Num,
	}
	sm.rsm.AddOp(op)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	config := sm.getLatestConfig()
	if args.Num == -1 || args.Num > config.Num {
		reply.Config = config
	} else {
		reply.Config = sm.configs[args.Num]
	}
	return nil
}

//
// Execute operation encoded in decided value v and update local state
//

func (sm *ShardMaster) joinReassign(shards [common.NShards]int64, acceptor int64) [common.NShards]int64 {
	var donor map[int64]int
	for i := range shards {
		if _, ok := donor[shards[i]]; !ok {
			donor[shards[i]] = 0
		}
	}
	// change distribution
	groupSize := len(sm.impl.ShardDistribution)
	if groupSize < common.NShards {
		acceptVal := common.NShards / (groupSize + 1)
		cnt := acceptVal
		for group, val := range sm.impl.ShardDistribution {
			if cnt == 0 {
				break
			}
			if val > acceptVal {
				sm.impl.ShardDistribution[group] -= 1
				donor[group] += 1
				cnt -= 1
			}
		}
		sm.impl.ShardDistribution[acceptor] = acceptVal
	} else {
		sm.impl.ShardDistribution[acceptor] = 0
	}
	for i := range shards {
		if donor[shards[i]] > 0 {
			shards[i] = acceptor
			donor[shards[i]] -= 1
		}
	}
	return shards
}

func (sm *ShardMaster) leaveReassign(shards [common.NShards]int64, donor int64) [common.NShards]int64 {
	var acceptor map[int64]int
	for group, _ := range sm.impl.ShardDistribution {
		acceptor[group] = 0
	}
	// change distribution
	groupSize := len(sm.impl.ShardDistribution)
	donateVal := sm.impl.ShardDistribution[donor]
	for group, _ := range sm.impl.ShardDistribution {
		if group == donor {
			delete(sm.impl.ShardDistribution, donor)
			break
		}
	}
	if groupSize <= common.NShards {
		acceptVal := common.NShards / (groupSize - 1)
		for group, val := range sm.impl.ShardDistribution {
			if donateVal == 0 {
				break
			}
			if val < acceptVal && donateVal >= acceptVal-val {
				donateVal -= acceptVal - val
				sm.impl.ShardDistribution[group] = acceptVal
				acceptor[group] += acceptVal - val
			}
		}
		if donateVal > 0 {
			for group, val := range sm.impl.ShardDistribution {
				if donateVal == 0 {
					break
				}
				if val == acceptVal {
					sm.impl.ShardDistribution[group] += 1
					acceptor[group] += 1
					donateVal -= 1
				}
			}
		}
	} else if donateVal > 0 {
		for group, val := range sm.impl.ShardDistribution {
			if val == 0 { // idle group
				sm.impl.ShardDistribution[group] = donateVal
				acceptor[group] = donateVal
				break
			}
		}
	}
	for i := range shards {
		if shards[i] == donor {
			for group, val := range acceptor {
				if val > 0 {
					shards[i] = group
					acceptor[group] -= 1
				}
			}
		}
	}
	return shards
}

func (sm *ShardMaster) ApplyOp(v interface{}) {
	op := v.(Op)
	if op.Operation == Query {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	lastConfig := sm.getLatestConfig()
	if op.Operation == Join {
		groups := make(map[int64][]string)
		for key, value := range lastConfig.Groups {
			groups[key] = value
		}
		groups[op.GID] = op.Servers

		var shards [common.NShards]int64
		if lastConfig.Num == 0 {
			for i := range shards {
				shards[i] = op.GID
			}
		} else {
			shards = sm.joinReassign(lastConfig.Shards, op.GID)
		}
		config := Config{
			Num:    lastConfig.Num + 1,
			Shards: shards,
			Groups: groups,
		}
		sm.configs = append(sm.configs, config)
	} else if op.Operation == Leave {
		groups := make(map[int64][]string)
		for key, value := range lastConfig.Groups {
			if key == op.GID {
				continue
			}
			groups[key] = value
		}

		shards := sm.leaveReassign(lastConfig.Shards, op.GID)
		config := Config{
			Num:    lastConfig.Num + 1,
			Shards: shards,
			Groups: groups,
		}
		sm.configs = append(sm.configs, config)
	} else {
		if _, ok := lastConfig.Groups[op.GID]; !ok {
			log.Printf("Move to unknown group!")
		}
		groups := make(map[int64][]string)
		for key, value := range lastConfig.Groups {
			groups[key] = value
		}

		shards := lastConfig.Shards
		oldGID := shards[op.Shard]
		shards[op.Shard] = op.GID
		sm.impl.ShardDistribution[oldGID] -= 1
		sm.impl.ShardDistribution[op.GID] += 1
	}
}
