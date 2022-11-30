package shardmaster

import (
	"log"
	"time"
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

func (sm *ShardMaster) findOptimalDistribution(donateVal int, leaver int64, operation int) (map[int64]int, map[int64]int) {
	donors := make(map[int64]int)
	acceptors := make(map[int64]int)
	groupSize := len(sm.impl.ShardDistribution)
	if groupSize <= 16 {
		newVal := common.NShards / groupSize
		balance := 0
		if operation == Leave {
			balance += donateVal
		}
		for group, val := range sm.impl.ShardDistribution {
			if val > newVal+1 {
				// find donors
				donors[group] = val - newVal - 1
				balance += val - newVal - 1
				sm.impl.ShardDistribution[group] = newVal + 1
			} else if val < newVal {
				// find acceptors
				acceptors[group] = newVal - val
				balance -= newVal - val
				sm.impl.ShardDistribution[group] = newVal
			}
		}
		if balance > 0 {
			for group, val := range sm.impl.ShardDistribution {
				if balance == 0 {
					break
				}
				if val == newVal {
					if _, isAcceptor := acceptors[group]; isAcceptor {
						acceptors[group] += 1
					} else {
						acceptors[group] = 1
					}
					balance -= 1
					sm.impl.ShardDistribution[group] = newVal + 1
				}
			}
		} else if balance < 0 {
			for group, val := range sm.impl.ShardDistribution {
				if balance == 0 {
					break
				}
				if val == newVal+1 {
					if _, isDonor := donors[group]; isDonor {
						donors[group] += 1
					} else {
						donors[group] = 1
					}
					balance += 1
					sm.impl.ShardDistribution[group] = newVal
				}
			}
		}
		if operation == Leave {
			donors[leaver] = donateVal
		}
	} else {
		newVal := 1
		balance := 0
		if operation == Leave {
			balance += donateVal
		}
		for group, val := range sm.impl.ShardDistribution {
			if val > newVal {
				donors[group] = val - newVal
				balance += val - newVal
				sm.impl.ShardDistribution[group] = newVal
			}
		}
		for group, val := range sm.impl.ShardDistribution {
			if balance == 0 {
				break
			}
			if val < newVal {
				acceptors[group] = newVal - val
				balance -= newVal - val
				sm.impl.ShardDistribution[group] = newVal
			}
		}
		if operation == Leave {
			donors[leaver] = donateVal
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
	// addition join todo: whether to rebalance when duplicate join
	if _, ok := sm.impl.ShardDistribution[joiner]; ok {
		return shards
	}
	// change distribution
	sm.impl.ShardDistribution[joiner] = 0
	donors, acceptors := sm.findOptimalDistribution(0, 0, Join)
	for i := range shards {
		if vd, ok := donors[shards[i]]; ok {
			if vd > 0 {
				donors[shards[i]] -= 1
				for acceptor, va := range acceptors {
					if va > 0 {
						shards[i] = acceptor
						acceptors[acceptor] -= 1
						break
					}
				}
			}
		}
	}
	return shards
}

func (sm *ShardMaster) leaveReassign(shards [common.NShards]int64, leaver int64) [common.NShards]int64 {
	// addition leave todo: rebalance
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
				donors[shards[i]] -= 1
				for acceptor, va := range acceptors {
					if va > 0 {
						shards[i] = acceptor
						acceptors[acceptor] -= 1
						break
					}
				}
			}
		}
	}
	return shards
}

//
// Execute operation encoded in decided value v and update local state
//

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
		// Part B
		donors := sm.findDonors(lastConfig, config)
		sendGroups := make(map[int64][]string)
		for k, v := range config.Groups {
			sendGroups[k] = v
		}
		if len(donors) == 0 && config.Num == 1 {
			database := make(map[string]string)
			handledId := make(map[int]bool)
			sm.sendAcceptRPC(config.Num, shards, database, handledId, sendGroups[shards[0]])
		} else if len(donors) > 0 {
			for i := range donors {
				acceptorDict := sm.findAcceptors(lastConfig, config, donors[i])
				sm.sendDonateRPC(config.Num, config.Shards, sendGroups, acceptorDict, sendGroups[donors[i]])
			}
		}
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
		// Part B
		donors := sm.findDonors(lastConfig, config)
		sendGroups := make(map[int64][]string)
		for k, v := range lastConfig.Groups {
			sendGroups[k] = v
		}
		for i := range donors {
			acceptorDict := sm.findAcceptors(lastConfig, config, donors[i])
			sm.sendDonateRPC(config.Num, config.Shards, sendGroups, acceptorDict, sendGroups[donors[i]])
		}
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
		// Part B
		donors := sm.findDonors(lastConfig, config)
		sendGroups := make(map[int64][]string)
		for k, v := range config.Groups {
			sendGroups[k] = v
		}
		for i := range donors {
			acceptorDict := sm.findAcceptors(lastConfig, config, donors[i])
			sm.sendDonateRPC(config.Num, config.Shards, sendGroups, acceptorDict, sendGroups[donors[i]])
		}
	}
}

func (sm *ShardMaster) sendDonateRPC(configNum int, shards [common.NShards]int64, groups map[int64][]string, acceptorDict map[int64][]int, servers []string) {
	requestId := int(common.Nrand())
	args := &common.DonateDataArgs{
		RequestId:    requestId,
		ConfigNum:    configNum,
		Shards:       shards,
		Groups:       groups,
		AcceptorDict: acceptorDict,
	}
	var reply common.DonateDataReply
	i := 0
	ok := common.Call(servers[i], "ShardKV.DonateData", args, &reply)
	for !ok || (ok && reply.Err != common.OK) {
		i += 1
		i = i % len(servers)
		time.Sleep(10 * time.Millisecond)
		ok = common.Call(servers[i], "ShardKV.DonateData", args, &reply)
	}
}

func (sm *ShardMaster) sendAcceptRPC(configNum int, shards [common.NShards]int64, database map[string]string, handledId map[int]bool, servers []string) {
	for i := 0; i < len(servers); i++ {
		requestId := int(common.Nrand())
		args := &common.AcceptDataArgs{
			RequestId: requestId,
			ConfigNum: configNum,
			Shards:    shards,
			Database:  database,
			HandledId: handledId,
		}
		var reply common.AcceptDataReply
		ok := common.Call(servers[i], "ShardKV.AcceptData", args, &reply)
		for !ok {
			time.Sleep(10 * time.Millisecond)
			ok = common.Call(servers[i], "ShardKV.AcceptData", args, &reply)
		}
	}
}

func (sm *ShardMaster) findAcceptors(lastConfig Config, config Config, donor int64) map[int64][]int {
	acceptorDict := make(map[int64][]int)
	lastShards := lastConfig.Shards
	shards := config.Shards
	for i := 0; i < common.NShards; i++ {
		if shards[i] != donor && lastShards[i] == donor {
			if v, ok := acceptorDict[shards[i]]; ok {
				acceptorDict[shards[i]] = append(v, i)
			} else {
				acceptorDict[shards[i]] = []int{i}
			}
		}
	}
	return acceptorDict
}

func (sm *ShardMaster) findDonors(lastConfig Config, config Config) []int64 {
	var donors []int64
	if lastConfig.Num == 0 {
		return donors
	}
	lastShards := lastConfig.Shards
	shards := config.Shards
	lastDict := sm.findDistribution(lastShards)
	dict := sm.findDistribution(shards)
	for group := range lastDict {
		if _, ok := lastDict[group]; ok {
			if _, ok := dict[group]; ok && lastDict[group] > dict[group] {
				donors = append(donors, group)
			} else if !ok {
				donors = append(donors, group)
			}
		}
	}
	return donors
}

func (sm *ShardMaster) findDistribution(shards [common.NShards]int64) map[int64]int {
	dict := make(map[int64]int)
	for i := range shards {
		if _, ok := dict[shards[i]]; ok {
			dict[shards[i]] += 1
		} else {
			dict[shards[i]] = 1
		}
	}
	return dict
}
