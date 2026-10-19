// cgo bridge between the Go package and sia_storage.h: declares the
// Go-exported callbacks and getters returning their C function pointers.
#ifndef SIA_STORAGE_GO_H
#define SIA_STORAGE_GO_H

#include "sia_storage.h"

// Names for the two callback types. The generated header writes them out at
// each use, as cbindgen does for a function pointer parameter, so there is no
// name to cast to, and the getters below need one.
//
// Both are invoked from arbitrary Rust runtime threads: an implementation must
// be thread-safe, must not call back into the library, and must not retain the
// pointed-to data past the call.
typedef void (*sia_progress_cb_t)(uintptr_t userdata, const sia_shard_progress_t *progress);
typedef void (*sia_log_cb_t)(uintptr_t userdata, int32_t level, const char *target, const char *message);

extern void goShardProgress(uintptr_t userdata, sia_shard_progress_t* progress);
extern void goLogMessage(uintptr_t userdata, int32_t level, char* target, char* message);

// The callback typedefs take their pointers as const. cgo writes these
// prototypes from the Go signatures and cannot const qualify them, so the
// getters cast. const carries no calling convention, so the call is the same.
static inline sia_progress_cb_t sia_go_progress_cb(void) { return (sia_progress_cb_t)goShardProgress; }
static inline sia_log_cb_t sia_go_log_cb(void) { return (sia_log_cb_t)goLogMessage; }

#endif // SIA_STORAGE_GO_H
