package siastorage

import (
	"errors"
	"time"

	"go.sia.tech/core/types"
)

// A ShardProgress reports the result of a successfully completed shard upload
// or download.
//
// Events are dropped when a handler cannot keep up.
type ShardProgress struct {
	HostKey    types.PublicKey
	SlabIndex  int
	ShardIndex int
	ShardSize  uint64
	Elapsed    time.Duration

	// Transferred is the running total for the transfer, counted before an
	// event can be dropped. Read it rather than summing ShardSize, which
	// loses whatever the handler missed.
	Transferred uint64
}

// The sentinels the FFI layer maps status codes and well known messages onto,
// so callers match with errors.Is rather than on message text.
var (
	// ErrRequestExpired is returned when a connection request has expired
	// before the user approved it.
	ErrRequestExpired = errors.New("connection request expired")

	// ErrUnauthorized is returned when the supplied app key is not authorized
	// by the indexer.
	ErrUnauthorized = errors.New("app key is not authorized")

	// ErrUserRejected is returned when the user rejects the connection
	// request.
	ErrUserRejected = errors.New("user rejected connection request")

	// ErrNotEnoughShards is returned when not enough shards were uploaded or
	// downloaded to satisfy the minimum required shards.
	ErrNotEnoughShards = errors.New("not enough shards")

	// ErrNoMoreHosts is returned when there are no more hosts available to
	// attempt to upload a shard.
	ErrNoMoreHosts = errors.New("no more hosts available")

	// ErrObjectNotAttached is returned when detaching an object from a sharing
	// key it was never attached to.
	ErrObjectNotAttached = errors.New("object is not attached to the sharing key")

	// ErrKeyMismatch is returned when a sharing key does not belong to the
	// account the call was made on.
	ErrKeyMismatch = errors.New("sharing key does not belong to this account")

	// ErrInvalidState is returned when a handle is used in a way its current
	// state does not allow, such as finishing an upload twice.
	ErrInvalidState = errors.New("invalid state for this operation")

	// ErrOutOfRange is returned when an upload's StartOffset lies past the end
	// of the object it would overwrite.
	ErrOutOfRange = errors.New("start offset is past the end of the object")
)
