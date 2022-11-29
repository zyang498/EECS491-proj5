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
	op1 := v1.(Op)
	op2 := v2.(Op)
	if op1.Operation != op2.Operation {
		return false
	} else {
		if op1.Operation == Join {
			if op1.GID == op2.GID {
				return true
			}
			return false
		} else if op1.Operation == Leave {
			if op1.GID == op2.GID {
				return true
			}
			return false
		} else if op1.Operation == Move {
			if op1.GID == op2.GID && op1.Shard == op2.Shard {
				return true
			}
			return false
		} else {
			if op1.ConfigNum == op2.ConfigNum {
				return true
			}
			return false
		}
	}
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

func (sm *ShardMaster) findOptimalDistribution(donateVal int, leaver int64, operation int) (map[int64]int, map[int64]int) {
	//log.Printf("%v, Init distribution is %v", operation, sm.impl.ShardDistribution)
	donors := make(map[int64]int)
	acceptors := make(map[int64]int)
	groupSize := len(sm.impl.ShardDistribution)
	if groupSize <= 16 {
		newVal := common.NShards / groupSize
		remainder := common.NShards % groupSize
		if operation == Leave {
			donors[leaver] = donateVal
		}
		for group, val := range sm.impl.ShardDistribution {
			if val < newVal {
				acceptors[group] = newVal - val
			} else if val > newVal {
				donors[group] = val - newVal
			}
			sm.impl.ShardDistribution[group] = newVal
		}
		if remainder > 0 {
			// todo: can be optimized
			for group, _ := range sm.impl.ShardDistribution {
				if remainder == 0 {
					break
				}
				sm.impl.ShardDistribution[group] += 1
				if _, isDonor := donors[group]; isDonor {
					donors[group] -= 1
				} else if _, isAcceptor := acceptors[group]; isAcceptor {
					acceptors[group] += 1
				} else {
					acceptors[group] = 1
				}
				remainder -= 1
			}
		}
	} else {
		newVal := 1
		totalDonation := 0
		if operation == Leave {
			totalDonation += donateVal
			donors[leaver] = donateVal
		}
		for group, val := range sm.impl.ShardDistribution {
			if val > newVal {
				donors[group] = val - newVal
				totalDonation += val - newVal
				sm.impl.ShardDistribution[group] = newVal
			}
		}
		for group, val := range sm.impl.ShardDistribution {
			if totalDonation == 0 {
				break
			}
			if val < newVal {
				acceptors[group] = newVal - val
				totalDonation -= newVal - val
				sm.impl.ShardDistribution[group] = newVal
			}
		}
	}
	// for debug
	d := 0
	a := 0
	for _, v := range donors {
		d += v
	}
	for _, v := range acceptors {
		a += v
	}
	if a != d {
		log.Printf("Donation %v isn't equal to accpetion %v!", donors, acceptors)
		log.Printf("Distribution is %v", sm.impl.ShardDistribution)
	}
	return donors, acceptors
}

func (sm *ShardMaster) joinReassign(shards [common.NShards]int64, joiner int64) [common.NShards]int64 {
	// addition join
	if _, ok := sm.impl.ShardDistribution[joiner]; ok {
		return shards
	}
	// change distribution
	sm.impl.ShardDistribution[joiner] = 0
	donors, acceptors := sm.findOptimalDistribution(0, 0, Join)
	for i := range shards {
		if vd, ok := donors[shards[i]]; ok {
			if vd > 0 {
				for acceptor, va := range acceptors {
					if va > 0 {
						shards[i] = acceptor
						acceptors[acceptor] -= 1
						break
					}
				}
			}
			donors[shards[i]] -= 1
		}
	}
	return shards
}

func (sm *ShardMaster) leaveReassign(shards [common.NShards]int64, leaver int64) [common.NShards]int64 {
	// addition leave
	if _, ok := sm.impl.ShardDistribution[leaver]; !ok {
		return shards
	}
	// change distribution
	donateVal := sm.impl.ShardDistribution[leaver]
	delete(sm.impl.ShardDistribution, leaver)
	donors, acceptors := sm.findOptimalDistribution(donateVal, leaver, Leave)
	for i := range shards {
		if vd, ok := donors[shards[i]]; ok {
			if vd > 0 {
				for acceptor, va := range acceptors {
					if va > 0 {
						shards[i] = acceptor
						acceptors[acceptor] -= 1
						break
					}
				}
			}
			donors[shards[i]] -= 1
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
			sm.impl.ShardDistribution[op.GID] = common.NShards
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
		config := Config{
			Num:    lastConfig.Num + 1,
			Shards: shards,
			Groups: groups,
		}
		sm.configs = append(sm.configs, config)
	}
}
