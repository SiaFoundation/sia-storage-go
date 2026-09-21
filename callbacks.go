package siastorage

/*
#include <stdint.h>
#include "sia_storage.h"
*/
import "C"

import (
	"time"
	"unsafe"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// goShardProgress is invoked by the Rust runtime for every completed shard
// transfer. It runs on a Rust thread; the registered handler must be fast and
// must not call back into the SDK.
//
//export goShardProgress
func goShardProgress(userdata C.uintptr_t, progress *C.sia_shard_progress_t) {
	fn := progressHandler(uintptr(userdata))
	if fn == nil {
		return
	}
	var hostKey types.PublicKey
	copy(hostKey[:], (*[32]byte)(unsafe.Pointer(&progress.host_key[0]))[:])
	fn(ShardProgress{
		HostKey:    hostKey,
		SlabIndex:  int(progress.slab_index),
		ShardIndex: int(progress.shard_index),
		ShardSize:  uint64(progress.shard_size),
		Elapsed:    time.Duration(progress.elapsed_us) * time.Microsecond,
	})
}

// goLogMessage bridges the Rust `log` crate to the configured zap logger.
//
//export goLogMessage
func goLogMessage(_ C.uintptr_t, level C.int32_t, target *C.char, message *C.char) {
	log := globalLogger.Load()
	if log == nil {
		return
	}
	msg := C.GoString(message)
	fields := []zap.Field{zap.String("target", C.GoString(target))}
	switch level {
	case 1:
		log.Error(msg, fields...)
	case 2:
		log.Warn(msg, fields...)
	case 3:
		log.Info(msg, fields...)
	default:
		log.Debug(msg, fields...)
	}
}
