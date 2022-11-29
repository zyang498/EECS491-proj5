package paxos

import "log"

// In all data types that represent RPC arguments/reply, field names
// must start with capital letters, otherwise RPC will break.

const (
	OK      = "OK"
	EmptyOK = "EmptyOK"
	Reject  = "Reject"
)

type Response string

type PrepareArgs struct {
	Seq int
	N   ProposalNumber
}

type PrepareReply struct {
	Seq      int
	Response Response
	Np       ProposalNumber
	Na       ProposalNumber
	Va       interface{}
}

type AcceptArgs struct {
	Seq int
	N   ProposalNumber
	V   interface{}
}

type AcceptReply struct {
	Seq      int
	Response Response
	N        ProposalNumber
}

type DecidedArgs struct {
	Seq int
	V   interface{}
	N   ProposalNumber
}

type DecidedReply struct {
	Seq      int
	Response Response
	Done     int
	me       int
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	// could be optimized: put lock into if statement
	px.mu.Lock()
	defer px.mu.Unlock()
	//log.Printf("Receive prepare on replica %v with seq %v proposal number %v from proposer %v", px.me, args.Seq, args.N.Number, args.N.Id)
	if _, isInNp := px.impl.np[args.Seq]; !isInNp {
		px.impl.np[args.Seq] = args.N
		reply.Response = EmptyOK
		reply.Seq = args.Seq
		reply.Np = args.N
	} else if args.N.Number > px.impl.np[args.Seq].Number {
		px.impl.np[args.Seq] = args.N
		reply.Seq = args.Seq
		reply.Np = args.N
		if _, isInNa := px.impl.na[args.Seq]; !isInNa {
			reply.Response = EmptyOK
		} else {
			reply.Response = OK
			reply.Na = px.impl.na[args.Seq]
			reply.Va = px.impl.va[args.Seq]
		}
	} else {
		reply.Response = Reject
		reply.Seq = args.Seq
		reply.Np = px.impl.np[args.Seq]
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if args.N.Number > px.impl.np[args.Seq].Number {
		px.impl.np[args.Seq] = args.N
		px.impl.na[args.Seq] = args.N
		px.impl.va[args.Seq] = args.V
		reply.Response = OK
		reply.Seq = args.Seq
		reply.N = args.N
	} else if args.N.Number == px.impl.np[args.Seq].Number {
		if args.N.Id == px.impl.np[args.Seq].Id {
			px.impl.np[args.Seq] = args.N
			px.impl.na[args.Seq] = args.N
			px.impl.va[args.Seq] = args.V
			reply.Response = OK
			reply.Seq = args.Seq
			reply.N = args.N
		} else {
			log.Printf("Proposer %v and Proposer %v both enter accept phase with proposal number %v", px.impl.np[args.Seq].Id, args.N.Id, args.N.Number)
			reply.Response = Reject
			reply.Seq = args.Seq
			reply.N = args.N
		}
	} else {
		reply.Response = Reject
		reply.Seq = args.Seq
		reply.N = args.N
	}
	return nil
}

func (px *Paxos) Learn(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	// if Seq not in log and Seq is greater than local highest done seq number, succeed
	if args.Seq > px.impl.localDone {
		if _, ok := px.impl.instanceLog[args.Seq]; ok {
			px.impl.instanceLog[args.Seq] = args.V
			reply.Response = OK
			reply.me = px.me
			reply.Done = px.impl.localDone
		} else {
			px.impl.instanceLog[args.Seq] = args.V
			reply.Response = OK
			reply.me = px.me
			reply.Done = px.impl.localDone
		}
	} else {
		//log.Printf("Requested decide for Seq %v on replica %v but localDone is %v", args.Seq, px.me, px.impl.localDone)
		reply.Response = Reject
		reply.me = px.me
		reply.Done = px.impl.localDone
	}
	return nil
}

//
// add RPC handlers for any RPCs you introduce.
//

// get localDone
