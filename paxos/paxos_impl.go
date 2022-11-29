package paxos

import (
	"time"
	"umich.edu/eecs491/proj5/common"
)

const (
	StartProposalNumber = 0
	InitDone            = -1
)

type ProposalNumber struct {
	Number int
	Id     int
}

//
// additions to Paxos state.
//
type PaxosImpl struct {
	// a log of instances agreement
	instanceLog map[int]interface{}
	// np, na, va
	np map[int]ProposalNumber
	na map[int]ProposalNumber
	va map[int]interface{}
	// local highest done seq number (init -1)
	localDone int
	// universal highest done seq number (init -1)
	peersDone []int
	// local state
}

//
// your px.impl.* initializations here.
//
func (px *Paxos) initImpl() {
	px.impl.instanceLog = make(map[int]interface{})
	px.impl.np = make(map[int]ProposalNumber)
	px.impl.na = make(map[int]ProposalNumber)
	px.impl.va = make(map[int]interface{})
	px.impl.localDone = InitDone
	numPeers := len(px.peers)
	px.impl.peersDone = make([]int, numPeers)
	for i := 0; i < numPeers; i++ {
		px.impl.peersDone[i] = InitDone
	}
}

func FindMaxProposal(seen_np []int) int {
	var maxNumber int
	for idx, e := range seen_np {
		if idx == 0 || e > maxNumber {
			maxNumber = e
		}
	}
	return maxNumber
}

func FindValue(seen_na []ProposalNumber, seen_va []interface{}, majority int) (int, bool) {
	var maxNumber ProposalNumber
	var maxNumberIdx int
	cnt := 0
	for idx, e := range seen_na {
		if idx == 0 || e.Number > maxNumber.Number {
			cnt = 0
			maxNumberIdx = idx
			maxNumber = e
		} else if e.Number == maxNumber.Number && e.Id != maxNumber.Id {
			//log.Printf("Replica %v and %v finish accept phase on same proposal number %v", e.Id, maxNumber.Id, e.Number)
		} else if e.Number == maxNumber.Number && e.Id == maxNumber.Id {
			if seen_va[maxNumberIdx] == seen_va[idx] {
				cnt += 1
			}
		}
	}
	if cnt >= majority {
		return maxNumberIdx, true
	} else {
		return maxNumberIdx, false
	}
}

// todo: local prepare?

func (px *Paxos) LocalAccept(seq int, v interface{}, n ProposalNumber) bool {
	px.mu.Lock()
	defer px.mu.Unlock()
	if _, ok := px.impl.np[seq]; !ok {
		px.impl.np[seq] = n
		px.impl.na[seq] = n
		px.impl.va[seq] = v
		return true
	}
	if n.Number > px.impl.np[seq].Number {
		px.impl.np[seq] = n
		px.impl.na[seq] = n
		px.impl.va[seq] = v
		return true
	} else if n.Number == px.impl.np[seq].Number {
		if n.Id == px.impl.np[seq].Id {
			px.impl.np[seq] = n
			px.impl.na[seq] = n
			px.impl.va[seq] = v
			return true
		} else {
			//log.Printf("Proposer %v and Proposer %v both enter accept phase on seq %v with proposal number %v", px.impl.np[seq].Id, n.Id, seq, n.Number)
			return false
		}
	} else {
		return false
	}
}

func (px *Paxos) LocalLearn(seq int, v interface{}, n ProposalNumber) bool {
	px.mu.Lock()
	defer px.mu.Unlock()
	// if Seq not in log and Seq is greater than local highest done seq number, succeed
	if seq > px.impl.localDone {
		if _, ok := px.impl.instanceLog[seq]; ok {
			if px.impl.instanceLog[seq] == v {
				px.impl.np[seq] = n
				px.impl.na[seq] = n
				px.impl.va[seq] = v
				return true
			} else {
				//log.Printf("Requested decide for Seq %v on replica %v but Seq already in log with different value %s", seq, px.me, px.impl.instanceLog[seq])
				return false
			}
		} else {
			px.impl.np[seq] = n
			px.impl.na[seq] = n
			px.impl.va[seq] = v
			px.impl.instanceLog[seq] = v
			return true
		}
	} else {
		//log.Printf("Requested decide for Seq %v on replica %v but localDone is %v", seq, px.me, px.impl.localDone)
		return false
	}
}

func (px *Paxos) PreparePhase(seq int, v interface{}, seen_np *[]int, n *ProposalNumber) (bool, interface{}, bool) {
	px.mu.Lock()
	var seen_na []ProposalNumber
	var seen_va []interface{}
	mojorityCopy := len(px.peers)/2 + 1
	majority := len(px.peers)/2 + 1
	if len(*seen_np) == 0 {
		if _, isPromised := px.impl.np[seq]; isPromised {
			n.Number = px.impl.np[seq].Number + 1
		} else {
			n.Number = StartProposalNumber
			//defer px.mu.Unlock()
			//return true, v
		}
	} else {
		n.Number = FindMaxProposal(*seen_np) + 1
	}
	px.mu.Unlock()
	// send prepare to all servers
	for _, replica := range px.peers {
		var prepareArgs = new(PrepareArgs)
		var prepareReply = new(PrepareReply)
		prepareArgs.Seq = seq
		prepareArgs.N = *n
		ok := common.Call(replica, "Paxos.Prepare", prepareArgs, prepareReply)
		if ok {
			*seen_np = append(*seen_np, prepareReply.Np.Number)
			if prepareReply.Response == OK {
				majority -= 1
				seen_na = append(seen_na, prepareReply.Na)
				seen_va = append(seen_va, prepareReply.Va)
			} else if prepareReply.Response == EmptyOK {
				majority -= 1
			}
			if majority <= 0 {
				if len(seen_na) == 0 {
					return true, v, false
				} else {
					idx, canSkipAccept := FindValue(seen_na, seen_va, mojorityCopy)
					if canSkipAccept {
						return true, seen_va[idx], canSkipAccept
					}
				}
			}
		}
	}
	if majority <= 0 {
		if len(seen_na) == 0 {
			return true, v, false
		} else {
			idx, canSkipAccept := FindValue(seen_na, seen_va, mojorityCopy)
			return true, seen_va[idx], canSkipAccept
		}
	} else {
		return false, v, false
	}
}

func (px *Paxos) AcceptPhase(seq int, v interface{}, n ProposalNumber) bool {
	majority := len(px.peers)/2 + 1
	for idx, replica := range px.peers {
		if idx == px.me {
			if px.LocalAccept(seq, v, n) {
				majority -= 1
			}
			if majority <= 0 {
				return true
			}
			continue
		}
		var acceptArgs = new(AcceptArgs)
		var acceptReply = new(AcceptReply)
		acceptArgs.Seq = seq
		acceptArgs.N = n
		acceptArgs.V = v
		ok := common.Call(replica, "Paxos.Accept", acceptArgs, acceptReply)
		if ok {
			if acceptReply.Response == OK {
				majority -= 1
			}
			if majority <= 0 {
				return true
			}
		}
	}
	if majority <= 0 {
		return true
	} else {
		return false
	}
}

func (px *Paxos) LearnPhase(seq int, v interface{}, n ProposalNumber) {
	for idx, replica := range px.peers {
		if idx == px.me {
			px.LocalLearn(seq, v, n)
			continue
		}
		var decidedArgs = new(DecidedArgs)
		var decidedReply = new(DecidedReply)
		decidedArgs.Seq = seq
		decidedArgs.V = v
		decidedArgs.N = n
		ok := common.Call(replica, "Paxos.Learn", decidedArgs, decidedReply)
		if ok {
			px.mu.Lock()
			px.impl.peersDone[idx] = decidedReply.Done
			px.mu.Unlock()
		}
	}
	px.Forget()
}

func (px *Paxos) Proposer(seq int, v interface{}) {
	var seen_np []int
	var n ProposalNumber
	n.Id = px.me
	for !px.isdead() {
		px.mu.Lock()
		_, isDecided := px.impl.instanceLog[seq]
		px.mu.Unlock()
		if isDecided {
			break
		}
		// phase 1: Prepare
		isPrepare := false
		canSkipAccept := false
		for !isPrepare {
			duration := time.Duration(1 * (2 * px.me))
			time.Sleep(duration * time.Millisecond)
			isPrepare, v, canSkipAccept = px.PreparePhase(seq, v, &seen_np, &n)
		}
		// phase 2: Accept
		//log.Printf("Start accept on proposer %v for seq %v with value %v", px.me, seq, v)
		if !canSkipAccept {
			isAccept := px.AcceptPhase(seq, v, n)
			if !isAccept {
				continue
			}
		}
		// phase 3: Learn
		//log.Printf("Start learn on proposer %v for seq %v with value %v", px.me, seq, v)
		px.LearnPhase(seq, v, n)
		break
	}
}

func (px *Paxos) Forget() {
	// forget log that is fewer than universal highest done seq number
	universalMin := px.Min()
	px.mu.Lock()
	for k, _ := range px.impl.instanceLog {
		if k < universalMin {
			delete(px.impl.instanceLog, k)
		}
	}
	for k, _ := range px.impl.np {
		if k < universalMin {
			delete(px.impl.np, k)
		}
	}
	for k, _ := range px.impl.na {
		if k < universalMin {
			delete(px.impl.na, k)
		}
	}
	for k, _ := range px.impl.va {
		if k < universalMin {
			delete(px.impl.va, k)
		}
	}
	px.mu.Unlock()
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Start a thread for proposer
	if !px.isdead() {
		go px.Proposer(seq, v)
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// update done seq number
	px.mu.Lock()
	px.impl.localDone = seq
	px.impl.peersDone[px.me] = seq
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	if len(px.impl.instanceLog) == 0 {
		return -1
	}
	maxNumber := -1
	for seq, _ := range px.impl.instanceLog {
		if seq > maxNumber {
			maxNumber = seq
		}
	}
	// for seq, _ := range px.impl.np {
	// 	if seq > maxNumber {
	// 		maxNumber = seq
	// 	}
	// }
	// for seq, _ := range px.impl.na {
	// 	if seq > maxNumber {
	// 		maxNumber = seq
	// 	}
	// }
	return maxNumber
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done(). The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	var minNumber int
	for idx, e := range px.impl.peersDone {
		if idx == 0 || e < minNumber {
			minNumber = e
		}
	}
	return minNumber + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so, what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	// check local log to determine whether seq has been decided
	if _, ok := px.impl.instanceLog[seq]; ok {
		defer px.mu.Unlock()
		return Decided, px.impl.instanceLog[seq]
	} else {
		if len(px.impl.instanceLog) == 0 {
			defer px.mu.Unlock()
			if seq <= px.impl.localDone {
				return Forgotten, nil
			}
			return Pending, nil
		} else {
			minSeq := -1
			for minSeq = range px.impl.instanceLog {
				break
			}
			for k := range px.impl.instanceLog {
				if k < minSeq {
					minSeq = k
				}
			}
			if seq < minSeq {
				px.mu.Unlock()
				return Forgotten, nil
			} else {
				px.mu.Unlock()
				return Pending, nil
			}
		}
	}
}

// Part B

func (px *Paxos) NextSeq() (int, bool) {
	nextSeq := -1
	localMax := px.Max()
	for idx, replica := range px.peers {
		if idx == px.me && localMax > nextSeq {
			nextSeq = localMax
			continue
		}
		var getMaxArgs = new(GetMaxArgs)
		var getMaxReply = new(GetMaxReply)
		ok := common.Call(replica, "Paxos.GetMax", getMaxArgs, getMaxReply)
		if ok && getMaxReply.Max > nextSeq {
			nextSeq = getMaxReply.Max
		}
	}
	isLocalMax := true
	if nextSeq > localMax {
		isLocalMax = false
	}
	nextSeq += 1
	return nextSeq, isLocalMax
}

func (px *Paxos) GetLocalDone() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.impl.localDone
}
