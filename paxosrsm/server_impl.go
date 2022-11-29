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
	currSeq int
	mu      sync.Mutex
}

//
// initialize rsm.impl.*
//
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl.currSeq = 0
}

//
// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
//
func (rsm *PaxosRSM) AddOp(v interface{}) {
	// callback
	rsm.impl.mu.Lock()
	defer rsm.impl.mu.Unlock()
	defer rsm.applyOp(v)
	for true {
		rsm.px.Start(rsm.impl.currSeq, v)
		to := 10 * time.Millisecond
		for {
			status, val := rsm.px.Status(rsm.impl.currSeq)
			if status == paxos.Decided {
				rsm.px.Done(rsm.impl.currSeq)
				rsm.impl.currSeq++
				if rsm.equals(val, v) { // successfully find a slot
					return
				} else {
					rsm.applyOp(val)
				}
				break
			} else if status == paxos.Forgotten {
				log.Println("Error: forgotten but still needed")
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		}
	}
}
