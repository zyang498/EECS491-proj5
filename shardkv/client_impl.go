package shardkv

//
// additions to Clerk state
//
type ClerkImpl struct {
}

//
// initialize ck.impl.*
//
func (ck *Clerk) InitImpl() {
}

//
// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	return ""
}

//
// send a Put or Append request.
// keep retrying forever until success.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
}
