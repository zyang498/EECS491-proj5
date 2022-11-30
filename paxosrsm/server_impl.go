package paxosrsm

import (
	"log"
	"sync"
	"time"
	"umich.edu/eecs491/proj5/paxos"
)

//
// additions to PaxosRSM state
//
type PaxosRSMImpl struct {
	mu  sync.Mutex
	seq int
}

//
// initialize rsm.impl.*
//
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl.seq = 0
}

//
// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
//
func (rsm *PaxosRSM) AddOp(v interface{}) {
	rsm.impl.mu.Lock()
	defer rsm.impl.mu.Unlock()
	for {
		rsm.px.Start(rsm.impl.seq, v)
		to := 10 * time.Millisecond
		for {
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			status, value := rsm.px.Status(rsm.impl.seq)
			if status == paxos.Pending {
				continue
			} else if status == paxos.Forgotten {
				log.Printf("----------------------Should never get here------------------------")
				rsm.impl.seq += 1
			} else {
				//log.Printf("2 seq %v value %v", rsm.impl.seq, value)
				//log.Printf("2 seq %v v %v", rsm.impl.seq, v)
				rsm.applyOp(value)
				rsm.impl.seq += 1
				if rsm.equals(v, value) {
					rsm.px.Done(rsm.impl.seq - 1)
					return
				} else {
					break
				}
			}
		}
	}
}
