package common

//
// define here any data types that you need to access in two packages without
// creating circular dependencies
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type DonateDataArgs struct {
	RequestId    int
	ConfigNum    int
	Shards       [NShards]int64
	Groups       map[int64][]string
	AcceptorDict map[int64][]int
}

type DonateDataReply struct {
	Err Err
}

type AcceptDataArgs struct {
	RequestId int
	ConfigNum int
	Shards    [NShards]int64
	Database  map[string]string
	HandledId map[int]bool
}

type AcceptDataReply struct {
	Err Err
}

func Contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
