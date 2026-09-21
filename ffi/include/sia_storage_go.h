// cgo bridge between the Go package and sia_storage.h: declares the
// Go-exported callbacks and getters returning their C function pointers.
#ifndef SIA_STORAGE_GO_H
#define SIA_STORAGE_GO_H

#include "sia_storage.h"

extern void goShardProgress(uintptr_t userdata, sia_shard_progress_t* progress);
extern void goLogMessage(uintptr_t userdata, int32_t level, char* target, char* message);

static inline sia_progress_cb_t sia_go_progress_cb(void) { return goShardProgress; }
static inline sia_log_cb_t sia_go_log_cb(void) { return goLogMessage; }

#endif // SIA_STORAGE_GO_H
