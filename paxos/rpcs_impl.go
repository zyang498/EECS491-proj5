package paxos

// In all data types that represent RPC arguments/reply, field names 
// must start with capital letters, otherwise RPC will break.

const (
	OK          = "OK"
	Reject      = "Reject"
)

type Response string

type PrepareArgs struct {
}

type PrepareReply struct {
}

type AcceptArgs struct {
}

type AcceptReply struct {
}

type DecidedArgs struct {
}

type DecidedReply struct {
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	return nil
}

func (px *Paxos) Learn(args *DecidedArgs, reply *DecidedReply) error {
	return nil
}

//
// add RPC handlers for any RPCs you introduce.
//
