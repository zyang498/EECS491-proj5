package common

//
// define here any data types that you need to access in two packages without
// creating circular dependencies
//

type DonateDataArgs struct {
	RequestId int
	ConfigNum int
	Shards    [NShards]int64
	Groups    map[int64][]string
}

type DonateDataReply struct {
}

func Contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
